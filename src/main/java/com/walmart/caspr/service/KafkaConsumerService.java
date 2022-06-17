package com.walmart.caspr.service;

import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

public interface KafkaConsumerService {
    Mono<ReceiverRecord<String, String>> processRecordRetryLazily(ReceiverRecord<String, String> receiverRecord);

    Mono<ReceiverRecord<String, String>> processRecord(ReceiverRecord<String, String> receiverRecord);

    void handleSuccess(ReceiverRecord<String, String> record);
}
