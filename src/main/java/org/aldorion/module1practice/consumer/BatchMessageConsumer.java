package org.aldorion.module1practice.consumer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aldorion.module1practice.config.KafkaConfigurationProperties;
import org.aldorion.module1practice.model.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class BatchMessageConsumer {

    private final KafkaConfigurationProperties kafkaConfigurationProperties;
    private Properties properties;
    // volatile делает сигнал остановки видимым потоку, который читает сообщения.
    private volatile boolean running = true;
    private KafkaConsumer<String, Message> consumer;

    // stop() вызывается из потока завершения приложения. Тот же монитор this
    // используется при присваивании и обнулении consumer: ссылка видна между потоками,
    // а проверка и wakeup() не могут пересечься с обнулением ссылки перед close().
    public synchronized void stop() {
        running = false;
        // wakeup() можно вызвать из другого потока: он прерывает ожидание Kafka
        // через WakeupException. Сам close() выполнит поток чтения в finally.
        if (consumer != null) consumer.wakeup();
    }

    // Подготовка конфигурации для создания консьюмера
    @PostConstruct
    private void init() {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigurationProperties.getBootstrapServer());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class.getName());
        properties.put(JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, Message.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().groupId()); //batchMessageConsumer
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().autoOffsetReset()); //latest - выбрал просто по своему желанию.
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().enableAutoCommit()); //false - по требованию задания
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().sessionTimeoutMs()); //120000 - сколько брокер ждёт heartbeat, прежде чем исключить консьюмера из группы. Тоже по своему желанию.
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().fetchMinBytes()); // 4000 - вычислил опытным путем, что б было более 10 сообщений в одном батче.
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().fetchMaxWaitMs()); // 60000 - вычислил опытным путем, если fetchMinBytes не выполнится, за 60 секунд нужное число сообщений должно появиться.
        properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaConfigurationProperties.getBatchMessageConsumerConfig().requestTimeoutMs()); // 120000 - максимальное время, которое консьюмер ждёт ответ от брокера на отдельный запрос. Сделал специально в два раза больше 60000, что б никак не препятствовать набору нужного количества сообщения для батча
    }

    // Создание и запуск работы консьюмера
    public void createAndRunConsumer() {
        KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(properties);
        // Публикуем ссылку для stop() под тем же монитором.
        // Если stop() уже был вызван, running == false и цикл чтения не начнётся.
        synchronized (this) {
            this.consumer = consumer;
        }
        try {
            consumer.subscribe(List.of(kafkaConfigurationProperties.getTopicName()));

            while (running) {
                try {
                    ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(500));

                    if (records.isEmpty()) continue;

                    log.info("Получено {} сообщений", records.count());
                    for (ConsumerRecord<String, Message> record : records) {
                        log.info("Получено сообщение: key = {}, value = {}, offset = {}", record.key(), record.value(), record.offset());
                    }

                    consumer.commitSync();
                } catch (WakeupException e) {
                    // После stop() это штатный сигнал: следующая проверка running завершит цикл.
                    // Неожиданный wakeup при продолжающейся работе передаём выше.
                    if (running) throw e;
                } catch (RecordDeserializationException e) {
                    log.error("Ошибка десериализации сообщения из JSON: topic = {}, partition = {}, offset = {}",
                            e.topicPartition().topic(), e.topicPartition().partition(), e.offset(), e);
                    consumer.seek(e.topicPartition(), e.offset() + 1); // Пока пропускаем сообщения, с которыми возникла проблема десереализации.
                } catch (Exception e) {
                    log.error("Ошибка консьюмера {}", getClass().getSimpleName(), e);
                }
            }
        } finally {
            // Убираем ссылку под монитором stop(), чтобы он больше не вызывал wakeup().
            synchronized (this) {
                this.consumer = null;
            }
            // Закрываем консьюмер в потоке чтения, когда работа с ним уже закончена.
            consumer.close(Duration.ofSeconds(5));
        }
    }
}
