package com.walmart.caspr.service.impl;

import com.walmart.caspr.config.CasprKafkaReceiver;
import com.walmart.caspr.config.ThreadPoolConfig;
import com.walmart.caspr.service.IConsume;
import com.walmart.caspr.service.KafkaConsumerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Properties;

@Service
public class KafkaHttpClientService {

    private final Logger log = Loggers.getLogger(KafkaHttpClientService.class);

    private final CasprKafkaReceiver casprKafkaReceiver;
    private final ThreadPoolConfig threadPoolConfig;

    private final KafkaConsumerService kafkaConsumerService;

    @Value("${spring.kafka.max.retries}")
    private int maxRetries;
    @Value("${spring.kafka.max.retry.delay}")
    private long maxRetryDelay;


    public KafkaHttpClientService(CasprKafkaReceiver casprKafkaReceiver, ThreadPoolConfig threadPoolConfig, KafkaConsumerService kafkaConsumerService) {
        this.casprKafkaReceiver = casprKafkaReceiver;
        this.threadPoolConfig = threadPoolConfig;
        this.kafkaConsumerService = kafkaConsumerService;

        casprKafkaReceiver.init(readKafkaConfiguration(), new ConsumeService());
        log.info("KafkaReceiver started from KafkaHttpClientService");
    }

    private Properties readKafkaConfiguration() {
        Properties props = new Properties();
        props.setProperty("BootStrapServer", "localhost:9092");
        props.setProperty("clientIdConfig", "clientI");
        props.setProperty("groupId", "groupI");
        props.setProperty("topicName", "test-topic");
        return props;
    }

    public class ConsumeService implements IConsume {
        @Override
        public void onReceiveMessages(Flux<ReceiverRecord<String, String>> recordFlux) {

            Scheduler scheduler = Schedulers.newBoundedElastic(threadPoolConfig.getThreadCap(),
                    threadPoolConfig.getQueuedTaskCap(), threadPoolConfig.getThreadPrefix(), threadPoolConfig.getTtlSeconds());

            recordFlux.groupBy(m -> m.receiverOffset().topicPartition())
                    .doOnNext(partitionFlux -> log.info("processEvent:: topicPartition {}", partitionFlux.key()))
                    .flatMap(partitionFlux -> partitionFlux.subscribeOn(scheduler)
                            .doOnNext(r -> log.info("processEvent:: Record received from offset {} from topicPartition {} with message key {}", r.receiverOffset().topicPartition(), r.key(), r.offset()))
                            .flatMap(kafkaConsumerService::processRecordRetryLazily)
                            .doOnNext(receiverRecordInfo -> log.info("processEvent:: Record processed from offset {} from topicPartition {} with message key {}", receiverRecordInfo.receiverOffset().offset(), receiverRecordInfo.receiverOffset().topicPartition()))
                            .retryWhen(Retry.backoff(maxRetries, Duration.ofMillis(maxRetryDelay))
                            )
                            .onErrorResume(throwable -> Mono.empty())
                    )
                    .subscribe(
                            key -> log.info("Successfully consumed messages, key {}", key),
                            error -> log.error("Error while consuming messages ", error));

        }
    }

}
