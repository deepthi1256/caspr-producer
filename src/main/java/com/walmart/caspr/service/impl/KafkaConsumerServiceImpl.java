package com.walmart.caspr.service.impl;

import com.walmart.caspr.config.CasprKafkaProducer;
import com.walmart.caspr.model.api.ErrorRecord;
import com.walmart.caspr.service.CasprEventProcessor;
import com.walmart.caspr.service.KafkaConsumerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService {

    private final Logger log = Loggers.getLogger(KafkaConsumerServiceImpl.class);
    private final CasprKafkaProducer kafkaProducerService;
    private final CasprEventProcessor casprEventProcessor;
    private final DataTransformServiceImpl dataTransformService;
    private String errorRecord;
    @Value("${spring.kafka.max.retries}")
    private int maxRetries;

    public KafkaConsumerServiceImpl(CasprKafkaProducer kafkaProducerService, CasprEventProcessor casprEventProcessor, DataTransformServiceImpl dataTransformService, ErrorRecord errorRecord) {
        this.kafkaProducerService = kafkaProducerService;
        this.casprEventProcessor = casprEventProcessor;
        this.dataTransformService = dataTransformService;
    }

    @Override
    public Mono<ReceiverRecord<String, String>> processRecordRetryLazily(ReceiverRecord<String, String> receiverRecord) {
        AtomicInteger retryCount = new AtomicInteger(0);
        AtomicReference<ReceiverRecord<String, String>> currentPayload = new AtomicReference<>(receiverRecord);
        return Mono.defer(() -> processRecord(receiverRecord)
                        .doOnError(throwable -> retryCount.incrementAndGet())
                )
                .retry(maxRetries)
                .doOnError(throwable -> {

                    int currentRetryCount = retryCount.get() - 1;
                    log.info("processRecordRetryLazily:: Current retry count {}", currentRetryCount);

                    if (currentRetryCount == maxRetries) {
                        ReceiverRecord<String, String> failedRecord = currentPayload.get();
                        log.error("processRecordRetryLazily:: max retries exceeded for message key {} and publishing to DLQ..", failedRecord.key());

                        //TODO: publish to DLQ by merging failedRecord and throwable
                        errorRecord = dataTransformService.convertErrorMessageToRecord(failedRecord, throwable.getMessage());
                        kafkaProducerService.generateErrorMessage(errorRecord);
                        failedRecord.receiverOffset().acknowledge();
                    }

                })
                .doOnNext(record -> log.info("processRecordRetryLazily:: completed"));
    }

    @Override
    public Mono<ReceiverRecord<String, String>> processRecord(ReceiverRecord<String, String> receiverRecord) {
        return Mono.just(receiverRecord)
                .doOnNext(record -> log.info("processRecord:: processing started"))
                .flatMap(casprEventProcessor::deserializeJsonToHttpRequest)
                .flatMap(casprEventProcessor::submitPayloadToCasprAPI)
                .flatMap(casprEventProcessor::deserializeValidatorJsonToResponseBody)
                .map(casprEventProcessor::validateResponses)
                .doOnSuccess(this::handleSuccess);
    }

    @Override
    public void handleSuccess(ReceiverRecord<String, String> record) {
        record.receiverOffset().acknowledge();
        log.info("processRecord:: offset {} from topicPartition {} is acknowledged successfully", record.receiverOffset().offset(), record.receiverOffset().topicPartition());
    }
}
