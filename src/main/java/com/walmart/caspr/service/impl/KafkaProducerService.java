package com.walmart.caspr.service.impl;


import com.walmart.caspr.config.CasprKafkaProducer;
import com.walmart.caspr.config.ThreadPoolConfig;
import com.walmart.caspr.service.IProducer;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Properties;

@Service
public class KafkaProducerService {

    private final Logger logger = Loggers.getLogger(KafkaProducerService.class);
    private final CasprKafkaProducer casprKafkaProducer;
    private final ThreadPoolConfig threadPoolConfig;
    Properties props = new Properties();

    public KafkaProducerService(CasprKafkaProducer casprKafkaProducer, ThreadPoolConfig threadPoolConfig) {
        this.casprKafkaProducer = casprKafkaProducer;
        this.threadPoolConfig = threadPoolConfig;
        casprKafkaProducer.intializeProducer(readKafkaConfiguration(), new ProducerService());
        logger.info("KafkaProducer started from KafkaProducerService");
    }

    private Properties readKafkaConfiguration() {

        props.setProperty("BootStrapServer", "localhost:9092");
        props.setProperty("clientIdConfig", "clientTwo");
        props.setProperty("groupId", "groupTwo");
        props.setProperty("topicName", "error-record");
        return props;
    }

    public class ProducerService implements IProducer {
        @Override
        public void sendMessages(String record, String topicName,
                                 ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate) {
            Scheduler scheduler = Schedulers.newBoundedElastic(threadPoolConfig.getThreadCap(),
                    threadPoolConfig.getQueuedTaskCap(), threadPoolConfig.getThreadPrefix(), threadPoolConfig.getTtlSeconds());
            reactiveKafkaProducerTemplate.send(topicName, record).publishOn(scheduler)
                    .doOnSuccess(senderResult -> logger.info("sent message to offset : {} and partition={}",
                            senderResult.recordMetadata().offset(), senderResult.recordMetadata().partition()))
                    .subscribe();
        }
    }
}
