package com.basickafka.kafkafirst.exception;

public class NotRetryableException extends RuntimeException{

    public NotRetryableException(String message){
        super(message);

    }
}
