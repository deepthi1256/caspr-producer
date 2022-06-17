package com.walmart.caspr.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.walmart.caspr.exception.CasprAPIException;
import com.walmart.caspr.exception.ReceiverRecordException;
import com.walmart.caspr.model.HttpRequest;
import com.walmart.caspr.model.api.ErrorRecord;
import com.walmart.caspr.service.DataTransformService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.Exceptions;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
public class DataTransformServiceImpl implements DataTransformService {

    private final ObjectMapper objMap;

    @Autowired
    private final ErrorRecord err;
    private final Logger log = Loggers.getLogger(DataTransformServiceImpl.class);
    private String errorMessage;
    private Tuple2<ReceiverRecord<String, String>, HttpRequest> request;

    public DataTransformServiceImpl(ObjectMapper objMap, ErrorRecord err) {
        this.objMap = objMap;
        this.err = err;
    }

    @Override
    public Map<String, String> transformMapData(Map<String, Object> dataMap) {
        return Optional.ofNullable(dataMap)
                .filter(Objects::nonNull)
                .map(data -> data.entrySet()
                        .stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue())))
                .orElse(new HashMap<>());
    }


    @Override
    public String convertErrorMessageToRecord(ReceiverRecord<String, String> record, String throwable) {

        Mono.just(record)
                .doOnNext(recordValue -> log.info("convertErrorMessageToRecord:: processing started"))
                .flatMap(this::deserilaizetoGetPayload).
                map(m -> Tuples.of(m.getT2().getPayLoad(), record.topic(), throwable)).map(this::getErrorRecord).onErrorResume(e -> {
                    log.error("Failed to convert Message", e);
                    if (e instanceof CasprAPIException)
                        throw Exceptions.propagate(new ReceiverRecordException(record, e));
                    throw Exceptions.propagate(e);
                }).subscribe(v -> {
                    errorMessage = v;
                });
        return errorMessage;
    }

    public String getErrorRecord(Tuple3<String, String, String> tuple) {
        err.setResponse(tuple.getT1());
        err.setTopicName(tuple.getT2());
        err.setExceptionMessage(tuple.getT3());
        Mono.fromCallable(() -> objMap.writeValueAsString(err)).subscribe(v -> {
            errorMessage = v;
        });
        return errorMessage;
    }

    public Mono<Tuple2<ReceiverRecord<String, String>, HttpRequest>> deserilaizetoGetPayload(ReceiverRecord<String, String> record) {
        return Mono.fromCallable(() -> objMap.readValue(record.value(), HttpRequest.class))
                .doOnNext(httpRequest -> log.info("deserializeJsonToHttpRequest:: is httpRequest null? {}", Objects.isNull(httpRequest)))
                .map(httpRequest -> Tuples.of(record, httpRequest))
                .onErrorResume(e -> {
                    log.error("deserializeJsonToHttpRequest:: Exception while deserializing payload to HttpRequest..", e);
                    throw Exceptions.propagate(e);
                });
    }
}
