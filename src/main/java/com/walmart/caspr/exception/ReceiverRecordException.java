package com.walmart.caspr.exception;

import reactor.kafka.receiver.ReceiverRecord;

public class ReceiverRecordException extends RuntimeException {
    private final transient ReceiverRecord<String, String> receiverRecord;

    public ReceiverRecordException(ReceiverRecord<String, String> receiverRecord, Throwable t) {
        super(t);
        this.receiverRecord = receiverRecord;
    }

    public ReceiverRecord<String, String> getRecord() {
        return this.receiverRecord;
    }
}
