package com.walmart.caspr.exception;

public class EventConsumerException extends RuntimeException {

    public EventConsumerException(Throwable t) {
        super(t);
    }
}
