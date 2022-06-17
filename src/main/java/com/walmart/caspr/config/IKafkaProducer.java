package com.walmart.caspr.config;

import com.walmart.caspr.service.IProducer;

import java.util.Properties;

public interface IKafkaProducer {


    void intializeProducer(Properties props, IProducer produce);

    void generateErrorMessage(String errorRecord);

    void stop();
}
