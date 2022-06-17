package com.walmart.caspr.service;

import reactor.kafka.receiver.ReceiverRecord;

import java.util.Map;

public interface DataTransformService {

    Map<String, String> transformMapData(Map<String, Object> dataMap);

    String convertErrorMessageToRecord(ReceiverRecord<String, String> record, String errorDetails);
}
