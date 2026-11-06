package org.aldorion.module1practice.producer;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aldorion.module1practice.config.KafkaConfigurationProperties;
import org.aldorion.module1practice.model.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageProducer {

    private final KafkaConfigurationProperties kafkaConfigurationProperties;

    private Properties properties;
    // volatile позволяет потоку отправки увидеть сигнал остановки из другого потока.
    private volatile boolean running = true;

    // Остановка произойдёт при следующей проверке running, после текущей отправки и паузы.
    public void stop() {
        running = false;
    }

    @PostConstruct
    private void init() {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigurationProperties.getBootstrapServer());
        properties.put(ProducerConfig.ACKS_CONFIG, kafkaConfigurationProperties.getMessageProducerConfig().acks()); //all - для достижения At Least Once по требованию задания
        properties.put(ProducerConfig.RETRIES_CONFIG, kafkaConfigurationProperties.getMessageProducerConfig().retries()); //10 - количество ретраев отправки сообщения, при проблемах фиксации сообщения
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class.getName());
    }

    public void createAndRunProducer() {
        KafkaProducer<String, Message> producer = new KafkaProducer<>(properties);
        try {
            while (running) {
                ProducerRecord<String, Message> record = new ProducerRecord<>(
                        kafkaConfigurationProperties.getTopicName(),
                        UUID.randomUUID().toString(),
                        new Message(UUID.randomUUID().toString(), UUID.randomUUID().toString(), "Hello")
                );
                try {
                    log.info("Отправляемое сообщение: topic = {}, key = {}, value = {}",
                            record.topic(), record.key(), record.value());
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("Ошибка отправки сообщения: topic = {}, key = {}, value = {}",
                                    record.topic(), record.key(), record.value(), exception);
                        }
                    });
                } catch (SerializationException e) {
                    log.error("Ошибка сериализации сообщения в JSON: key = {}, value = {}",
                            record.key(), record.value(), e);
                } catch (Exception e) {
                    log.error("Ошибка отправки сообщения: topic = {}, key = {}, value = {}",
                            record.topic(), record.key(), record.value(), e);
                }
                Thread.sleep(2000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // Даём продюсеру до 10 секунд на завершение отправок и освобождение ресурсов.
            producer.close(Duration.ofSeconds(10));
        }
    }
}
