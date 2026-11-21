package org.aldorion.module2practice.moderation;

import org.aldorion.module1practice.model.Message;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import tools.jackson.core.type.TypeReference;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Правила обработки личных сообщений. Входящие сообщения лежат в топике Kafka
 * {@code messages}, а прошедшие проверку попадают в {@code filtered_messages}.
 */
@Configuration
@EnableKafkaStreams
public class ModerationTopology {
    // Словом считаем последовательность букв, комбинируемых знаков, цифр и подчёркиваний.
    private static final Pattern WORD = Pattern.compile("[\\p{L}\\p{M}\\p{N}_]+");

    @Bean
    KStream<String, Message> moderatedMessages(StreamsBuilder builder) {
        // Ключ - получатель. Например, для ivan@gmail.com список ["irina@gmail.com"]
        // означает, что Иван заблокировал сообщения от Ирины.
        GlobalKTable<String, Set<String>> blocked = rules(builder, "blocked_users");
        // По ключу global лежит один словарь запрещённых слов для всех получателей.
        GlobalKTable<String, Set<String>> dictionary = rules(builder, "banned_words");
        // Преобразуем JSON в Message при чтении и обратно в JSON при записи.
        JacksonJsonSerde<Message> messages = new JacksonJsonSerde<>(Message.class).ignoreTypeHeaders().noTypeInfo();

        KStream<String, Message> result = builder.stream("messages", Consumed.with(Serdes.String(), messages))
                .filter((key, message) -> isValid(message)) // Дальше проходят только сообщения, для которых isValid вернул true.
                .selectKey((key, m) -> m.to()) // Берём адрес получателя из поля to. Теперь это ключ записи в потоке.
                .leftJoin(blocked, (recipient, m) -> recipient, // leftJoin ищет по этому ключу список блокировок получателя.
                        (m, users) -> users != null && users.contains(m.from()) ? null : m) // Если в списке есть отправитель (from), возвращаем null, иначе - само сообщение. Если списка ещё нет, users равен null и сообщение тоже проходит.
                .filter((recipient, m) -> m != null) // Отбрасываем null. На этом обработка заблокированного сообщения заканчивается.
                .leftJoin(dictionary, (recipient, m) -> "global", ModerationTopology::censor); // Для остальных сообщений берём словарь global и передаём его вместе с сообщением в censor.
        result.to("filtered_messages", Produced.with(Serdes.String(), messages)); // Пишем результат в Kafka. Ключ - адрес получателя, значение - сообщение после проверки.
        return result;
    }

    private static boolean isValid(@Nullable Message message) {
        return message != null
                && message.from() != null && !message.from().isBlank()
                && message.to() != null && !message.to().isBlank()
                && message.text() != null;
    }

    // Когда из топика приходит запись с тем же ключом, новый список заменяет предыдущий.
    private GlobalKTable<String, Set<String>> rules(StreamsBuilder builder, String topic) {
        JacksonJsonSerde<Set<String>> lists = new JacksonJsonSerde<>(new TypeReference<Set<String>>() {}).ignoreTypeHeaders().noTypeInfo(); // JSON-массив читаем как Set<String> - набор строк, в котором удобно искать совпадения.

        return builder.globalTable(
                topic, Consumed.with(Serdes.String(), lists),
                Materialized.<String, Set<String>>as(Stores.persistentKeyValueStore(topic + "-store")).withKeySerde(Serdes.String()).withValueSerde(lists) // Сохраняем справочник на диске в RocksDB. Имя хранилища берём из имени топика.
        );
    }

    private static Message censor(Message message, Set<String> words) {
        if (words == null || words.isEmpty()) return message; // Пока словарь отсутствует или пуст, возвращаем исходное сообщение.
        // Например, при запрете «спам» строка «СПАМ, спамер!» превратится в «***, спамер!».
        // Сравниваем целое слово в нижнем регистре. У остальных слов сохраняем написание и пунктуацию.
        String text = WORD.matcher(message.text()).replaceAll(match ->
                words.contains(match.group().toLowerCase(Locale.ROOT)) ? "***" : match.group());

        return new Message(message.from(), message.to(), text);
    }
}