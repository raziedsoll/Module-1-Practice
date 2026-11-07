package org.aldorion.module1practice.model;

/**
 * Сообщение, которое передаётся через Kafka в формате JSON.
 */
public record Message(
        String from,
        String to,
        String text
) {
}
