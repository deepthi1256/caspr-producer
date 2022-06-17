package com.walmart.caspr.config;

import com.walmart.caspr.service.IConsume;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Component
public class CasprKafkaReceiver implements IKafka {

    private final Logger log = Loggers.getLogger(CasprKafkaReceiver.class);
    KafkaReceiver<String, String> kafkaReceiver;
    private IConsume consume;

    @Override
    public void init(Properties properties, IConsume consume) {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("BootStrapServer"));
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, properties.getProperty("clientIdConfig"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("groupId"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        createKafkaConfig(props, properties.getProperty("topicName"));

        this.consume = consume;
    }

    public void createKafkaConfig(Map<String, Object> props, String topicName) {
        ReceiverOptions<String, String> receiverOptions = ReceiverOptions.<String, String>create(props)
                .subscription(Collections.singleton(topicName));

        kafkaReceiver = KafkaReceiver.create(receiverOptions);
    }

    @Override
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        Flux<ReceiverRecord<String, String>> recordFlux = Flux.defer(() -> kafkaReceiver.receive());

        consume.onReceiveMessages(recordFlux);
    }


    @Override
    public void stop() {

    }


}
