package com.walmart.caspr.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "spring.kafka.consumer-thread-pool-config")
public class ThreadPoolConfig {
    private int threadCap;
    private int queuedTaskCap;
    private String threadPrefix;
    private int ttlSeconds;
}
