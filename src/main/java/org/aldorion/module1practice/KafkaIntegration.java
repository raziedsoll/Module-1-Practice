package org.aldorion.module1practice;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.aldorion.module1practice.consumer.BatchMessageConsumer;
import org.aldorion.module1practice.consumer.SingleMessageConsumer;
import org.aldorion.module1practice.producer.MessageProducer;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Класс для симуляции работы продюсера и консьюмеров.
 *
 * Используются виртуальные потоки для запуска продюсера и консьюмера.
 * Работа завершается при помощи PreDestroy для корректного вывода из работы продюсера и консьюмеров.
 */
@Component
@RequiredArgsConstructor
public class KafkaIntegration {

    private final MessageProducer messageProducer;
    private final SingleMessageConsumer singleMessageConsumer;
    private final BatchMessageConsumer batchMessageConsumer;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public void run() {
        executor.execute(messageProducer::createAndRunProducer);
        executor.execute(singleMessageConsumer::createAndRunConsumer);
        executor.execute(batchMessageConsumer::createAndRunConsumer);
    }

    // Spring вызывает метод при завершении приложения.
    @PreDestroy
    public void stop() {
        // Сначала просим задачи завершить циклы; консьюмеры также будят ожидание Kafka.
        messageProducer.stop();
        singleMessageConsumer.stop();
        batchMessageConsumer.stop();
        // Ждём завершения задач, включая закрытие Kafka-клиентов в их finally.
        executor.close();
    }
}
