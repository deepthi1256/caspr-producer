package com.walmart.caspr.service;

import com.walmart.caspr.model.HttpRequest;
import com.walmart.caspr.model.api.ResponseBody;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuple3;

public interface CasprEventProcessor {

    Mono<Tuple2<ReceiverRecord<String, String>, HttpRequest>> deserializeJsonToHttpRequest(ReceiverRecord<String, String> record);

    Mono<Tuple3<ReceiverRecord<String, String>, ResponseBody, ResponseBody>> deserializeValidatorJsonToResponseBody(Tuple3<ReceiverRecord<String, String>, HttpRequest, ResponseBody> tuple);

    Mono<Tuple3<ReceiverRecord<String, String>, HttpRequest, ResponseBody>> submitPayloadToCasprAPI(Tuple2<ReceiverRecord<String, String>, HttpRequest> tuple);

    ReceiverRecord<String, String> validateResponses(Tuple3<ReceiverRecord<String, String>, ResponseBody, ResponseBody> tuple);

}