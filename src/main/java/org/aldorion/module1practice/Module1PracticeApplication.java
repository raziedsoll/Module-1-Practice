package org.aldorion.module1practice;

import lombok.RequiredArgsConstructor;
import org.aldorion.module1practice.config.KafkaConfigurationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.CommandLineRunner;

@SpringBootApplication
@RequiredArgsConstructor
@EnableConfigurationProperties(KafkaConfigurationProperties.class)
public class Module1PracticeApplication implements CommandLineRunner {

    private final KafkaIntegration kafkaIntegration;

    public static void main(String[] args) {
        SpringApplication.run(Module1PracticeApplication.class, args);
    }

    @Override
    public void run(String... args) {
        kafkaIntegration.run();
    }
}
