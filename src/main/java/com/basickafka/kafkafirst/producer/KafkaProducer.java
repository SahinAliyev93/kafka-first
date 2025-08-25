package com.basickafka.kafkafirst.producer;


import com.basickafka.kafkafirst.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
@AllArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper mapper;

    @Transactional
    public void sendMessage(String topic, String message){
        kafkaTemplate.send(topic, message);
        System.out.println("Sending message: " + message + " to topic: " + topic);
    }

    @Transactional
    public <T> void sendMassages(String topic, Event<T> event ,String key){
        try {
            String payload = mapper.writeValueAsString(event);
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    key,
                    payload);
            record.headers().add("trace_id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
            kafkaTemplate.send(record);
            System.out.println("Sending event: " + payload + " to topic: " + topic);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send event to Kafka", e);
        }
    }
}
