package com.basickafka.kafkafirst;

import com.basickafka.kafkafirst.config.KafkaConfig;
import com.basickafka.kafkafirst.dto.Customer;
import com.basickafka.kafkafirst.event.Event;
import com.basickafka.kafkafirst.producer.KafkaProducer;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.sql.Timestamp;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
public class KafkaFirstApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaFirstApplication.class, args);
    }

   @Bean
    public ApplicationRunner runner (KafkaProducer producer) throws InterruptedException {

        return args -> {
         publishMessage(producer);
        };


    }


    private void publishMessage(KafkaProducer producer) {
        producer.sendMessage(KafkaConfig.ERROR_TOPIC,"EVENT - 1");
        producer.sendMessage(KafkaConfig.ERROR_TOPIC,"EVENT - 2");
        producer.sendMessage(KafkaConfig.ERROR_TOPIC,"EVENT - 2");

    }

    private void startNormnalTopic(KafkaProducer producer) throws InterruptedException{
        var customerId_1 = UUID.randomUUID();
        var customerId_2 = UUID.randomUUID();
        var customerId_3 = UUID.randomUUID();

        UUID[] customerIds = {customerId_1, customerId_2, customerId_3};

            while (true){
                int index = ThreadLocalRandom.current().nextInt(0,3);
                var customer = Customer.builder()
                        .customerId(customerIds[index])
                        .name("Sahin")
                        .email("sahin.aliyev979@gmail.com")
                        .phoneNumber("+99368234567")
                        .build();

                var event  = Event.<Customer>builder()
                        .eventId(UUID.randomUUID())
                        .eventType("Customer Created")
                        .createdDate(new Timestamp(System.currentTimeMillis()))
                        .payload(customer)
                        .build();
                producer.sendMassages(KafkaConfig.TOPIC_NAME,event,customer.getCustomerId().toString());
                Thread.sleep(1000L);
            }
    }
}
