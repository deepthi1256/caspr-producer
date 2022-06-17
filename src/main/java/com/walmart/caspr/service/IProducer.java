package com.walmart.caspr.service;

import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;

public interface IProducer {
    public void sendMessages(String document, String topicName, ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate);
}
