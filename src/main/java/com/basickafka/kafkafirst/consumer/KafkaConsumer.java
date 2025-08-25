package com.basickafka.kafkafirst.consumer;

import com.basickafka.kafkafirst.config.KafkaConfig;
import com.basickafka.kafkafirst.dto.Customer;
import com.basickafka.kafkafirst.event.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@AllArgsConstructor
public class KafkaConsumer {

    private final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    private final ObjectMapper mapper;

    @KafkaListener(id= "first_consumer",
            topics = KafkaConfig.TOPIC_NAME,
            groupId = "myGroup",
            concurrency = "2")
    public void listen(
            @Header(KafkaHeaders.RECEIVED_KEY) String key,
            @Header("trace_id") String traceId,
            @Header(KafkaHeaders.CONSUMER) Consumer<String, String> consumer,
            @Header(KafkaHeaders.RECEIVED_PARTITION) String partitionId,
            @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
            @Payload String payload
    ) throws JsonProcessingException {

    System.out.println("Key " + key + " traceId " + traceId);
        Event<Customer> event = null;
        try {
            event = mapper.readValue(payload, new TypeReference<>() {});

        logger.info("""
                Key: {}
                PartitionId: {}
                ConsumerId: {}
                ThreadId: {}
                event: {}
                ====================================================================================
                """, key,partitionId, consumer.groupMetadata().memberId(), Thread.currentThread().getId(), event);
        acknowledgment.acknowledge();
        acknowledgment.nack(Duration.ofSeconds(1));
        } catch (JsonProcessingException exception) {
            System.out.println("Exception in processing message " + exception.getMessage());
            throw exception;
        }
    }
}
