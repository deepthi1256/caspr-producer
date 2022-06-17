package com.walmart.caspr.service;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

public interface IConsume {

    void onReceiveMessages(Flux<ReceiverRecord<String, String>> recordFlux);

}
