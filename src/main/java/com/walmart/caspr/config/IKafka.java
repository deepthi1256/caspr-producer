package com.walmart.caspr.config;

import com.walmart.caspr.service.IConsume;

import java.util.Properties;

public interface IKafka {

    void init(Properties props, IConsume consume);


    void start();


    void stop();
}
