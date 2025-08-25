package com.basickafka.kafkafirst.config;


import com.basickafka.kafkafirst.exception.NotRetryableException;
import com.basickafka.kafkafirst.exception.RetryableException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;


@Configuration
public class KafkaConfig {

    public  static final String TOPIC_NAME = "first_topic";

    public  static final String ERROR_TOPIC = "error_topic";

    public  static final String FAIL_TOPIC = "failed_topic";


    @Bean
    public NewTopic topic(){
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic failedTopic(){
        return TopicBuilder.name(FAIL_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic errorTopic(){
        return TopicBuilder.name(ERROR_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public KafkaListenerErrorHandler errorHandler(){
        return (message,exception) -> {
            System.out.println("Error Handler caught exception: " + message);
           throw  exception;
            //return "FAILED";
        };
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactoryConfigurer(
            ConsumerFactory<String,String> consumerFactory,
            DefaultErrorHandler defaultErrorHandler)
    {
        var factory = new ConcurrentKafkaListenerContainerFactory<String,String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);
        factory.getContainerProperties().setDeliveryAttemptHeader(true);
        return factory;
    }

   @Bean
    public DefaultErrorHandler defaultErrorHandler(){
        var defaultErrorHandler = new DefaultErrorHandler(
                (consumerRecord, e) -> {
                    System.out.println("Default Error Handler caught exception: " + e.getMessage() + " for record: " + consumerRecord);
                },new FixedBackOff(1000L, 3));

        defaultErrorHandler.setAckAfterHandle(false);
        defaultErrorHandler.addNotRetryableExceptions(NotRetryableException.class);
        defaultErrorHandler.addRetryableExceptions(RetryableException.class);
        return defaultErrorHandler;
    }
}
