package com.walmart.caspr.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walmart.caspr.config.CasprKafkaProducer;
import com.walmart.caspr.exception.CasprAPIException;
import com.walmart.caspr.exception.ReceiverRecordException;
import com.walmart.caspr.model.HttpRequest;
import com.walmart.caspr.model.api.ResponseBody;
import com.walmart.caspr.service.CasperService;
import com.walmart.caspr.service.CasprEventProcessor;
import com.walmart.caspr.service.DataTransformService;
import com.walmart.caspr.service.ValidatorService;
import org.springframework.stereotype.Service;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.Objects;

@Service
public class CasprEventProcessorImpl implements CasprEventProcessor {

    private final Logger log = Loggers.getLogger(CasprEventProcessorImpl.class);

    private final ObjectMapper objectMapper;
    private final CasperService casperService;
    private final ValidatorService validatorService;
    private final CasprKafkaProducer kafkaProducerService;
    private final DataTransformService dataTransformService;
    private String errorRecord;

    public CasprEventProcessorImpl(ObjectMapper objectMapper, CasprKafkaProducer kafkaProducerService, CasperService casperService, ValidatorService validatorService, DataTransformService dataTransformService) {
        this.objectMapper = objectMapper;
        this.casperService = casperService;
        this.validatorService = validatorService;
        this.dataTransformService = dataTransformService;
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    public Mono<Tuple2<ReceiverRecord<String, String>, HttpRequest>> deserializeJsonToHttpRequest(ReceiverRecord<String, String> record) {
        return Mono.fromCallable(() -> objectMapper.readValue(record.value(), HttpRequest.class))
                .doOnNext(httpRequest -> log.info("deserializeJsonToHttpRequest:: is httpRequest null? {}", Objects.isNull(httpRequest)))
                .map(httpRequest -> Tuples.of(record, httpRequest))
                .onErrorResume(e -> {
                    log.error("deserializeJsonToHttpRequest:: Exception while deserializing payload to HttpRequest..", e);
                    throw Exceptions.propagate(e);
                });
    }

    @Override
    public Mono<Tuple3<ReceiverRecord<String, String>, ResponseBody, ResponseBody>> deserializeValidatorJsonToResponseBody(Tuple3<ReceiverRecord<String, String>, HttpRequest, ResponseBody> tuple) {
        return Mono.fromCallable(() -> objectMapper.readValue(tuple.getT2().getValidator().getPayLoad(), ResponseBody.class))
                .map(responseBody -> Tuples.of(tuple.getT1(), tuple.getT3(), responseBody))
                .onErrorResume(e -> {
                    log.error("deserializeValidatorJsonToResponseBody:: Exception while deserializing Validator payload to ResponseBody..", e);
                    throw Exceptions.propagate(e);
                });
    }

    @Override
    public Mono<Tuple3<ReceiverRecord<String, String>, HttpRequest, ResponseBody>> submitPayloadToCasprAPI(Tuple2<ReceiverRecord<String, String>, HttpRequest> tuple) {
        return casperService.postPayloadToCasper(tuple.getT2().getPayLoad(), tuple.getT2().getUrl(),
                        dataTransformService.transformMapData(tuple.getT2().getHeaders()),
                        dataTransformService.transformMapData(tuple.getT2().getCookies()))
                .doOnNext(responseBody -> log.info("submitPayloadToCasprAPI:: is responseBody null? {}", Objects.isNull(responseBody)))
                .doOnError(throwable -> log.error("submitPayloadToCasprAPI:: exception.. ", throwable))
                .map(responseBody -> Tuples.of(tuple.getT1(), tuple.getT2(), responseBody))
                .onErrorResume(e -> {
                    log.error("submitPayloadToCasprAPI:: Exception from Caspr API post call.", e);
                    if (e instanceof CasprAPIException)
                        throw Exceptions.propagate(new ReceiverRecordException(tuple.getT1(), e));
                    throw Exceptions.propagate(e);
                });
    }

    @Override
    public ReceiverRecord<String, String> validateResponses(Tuple3<ReceiverRecord<String, String>, ResponseBody, ResponseBody> tuple) {
        if (!validatorService.validateResponses(tuple.getT2(), tuple.getT3())) {
            //TODO: Publish to error topic as both responses did not match\
            errorRecord = dataTransformService.convertErrorMessageToRecord(tuple.getT1(), new Throwable("Error while Validating").getMessage());
            kafkaProducerService.generateErrorMessage(errorRecord);

            log.info("processRecord:: Publishing to error topic as both the responses did not match.. " +
                    "incoming validator payload {} and Caspr response {}", tuple.getT2(), tuple.getT3());
        }
        return tuple.getT1();
    }

}
