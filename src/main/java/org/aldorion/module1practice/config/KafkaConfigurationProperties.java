package org.aldorion.module1practice.config;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Класс конфигурации, для удобного конфигурирования работы с кластером Kafka
 *
 */
@ConfigurationProperties(prefix = "kafka")
@Getter
@RequiredArgsConstructor
public class KafkaConfigurationProperties {

    // Bootstrap сервер. Достаточно указать один адрес для доступа к кластеру.
    final String bootstrapServer;

    // Имя топика, с которым будут работать продюсер и консьюмеры.
    final String topicName;

    // Класс конфигурации для SingleMessageConsumer.
    final SingleMessageConsumerConfig singleMessageConsumerConfig;

    // Класс конфигурации для BatchMessageConsumer.
    final BatchMessageConsumerConfig batchMessageConsumerConfig;

    // Класс конфигурации для MessageProducer.
    final MessageProducerConfig messageProducerConfig;

    public record SingleMessageConsumerConfig(
            // Группа консьюмера.
            String groupId,
            // Позиция чтения при отсутствии сохранённого offset или его недоступности.
            String autoOffsetReset,
            // Автоматический коммит offsets.
            String enableAutoCommit,
            // Время без heartbeat, после которого брокер исключает консьюмера из группы, в мс.
            int sessionTimeoutMs,
            // Максимум сообщений за один poll().
            int maxPollRecords
    ) {
    }

    public record BatchMessageConsumerConfig(
            // Группа консьюмера.
            String groupId,
            // Позиция чтения при отсутствии сохранённого offset или его недоступности.
            String autoOffsetReset,
            // Автоматический коммит offsets.
            String enableAutoCommit,
            // Время без heartbeat, после которого брокер исключает консьюмера из группы, в мс.
            int sessionTimeoutMs,
            // Минимальный объём данных для ответа на FETCH-запрос к брокеру, в байтах.
            int fetchMinBytes,
            // Максимальное ожидание брокером накопления fetchMinBytes, в мс.
            int fetchMaxWaitMs,
            // Таймаут ожидания ответа на запрос, в мс.
            int requestTimeoutMs
    ) {

    }

    public record MessageProducerConfig(
            // Подтверждение записи.
            String acks,
            // Максимум повторных отправок при временных ошибках.
            int retries
    ) {

    }
}
