package com.basickafka.kafkafirst.exception;

public class RetryableException extends RuntimeException{

    public RetryableException(String message){
        super(message);

    }
}
