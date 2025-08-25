package com.basickafka.kafkafirst.event;

import lombok.Builder;

import java.sql.Timestamp;
import java.util.UUID;

@Builder
public record Event<T>(
        UUID eventId,
        String eventType,
        Timestamp createdDate,
        T payload
) {

}
