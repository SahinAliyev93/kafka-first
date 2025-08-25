package com.basickafka.kafkafirst.consumer;


import com.basickafka.kafkafirst.config.KafkaConfig;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

@Component
public class KafkaErrorConsumer {


    @KafkaListener(id = "error_consumer",
               topics = KafkaConfig.ERROR_TOPIC,
             groupId = "error-group",
             errorHandler = "errorHandler")
    @SendTo(KafkaConfig.FAIL_TOPIC)
    public void listenErrorTopic(
            @Header(KafkaHeaders.DELIVERY_ATTEMPT) int attempt
            ,@Payload String payload){
        System.out.println("Received message in error topic: " + payload +"attempt:"+attempt );
        throw  new RuntimeException("Simulated processing error");
    }
}
