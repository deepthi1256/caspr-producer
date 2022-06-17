package com.walmart.caspr.config;

import com.walmart.caspr.service.IProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Component;
import reactor.kafka.sender.SenderOptions;
import reactor.util.Logger;
import reactor.util.Loggers;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Component
public class CasprKafkaProducer implements IKafkaProducer {
    private final Logger log = Loggers.getLogger(CasprKafkaProducer.class);
    private final ThreadPoolConfig threadPoolConfig;
    String topicName = "";
    private ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;
    private IProducer produce;

    public CasprKafkaProducer(ThreadPoolConfig threadPoolConfig) {

        this.threadPoolConfig = threadPoolConfig;
    }

    @Override
    public void intializeProducer(Properties properties, IProducer produce) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("BootStrapServer"));
        props.put(ProducerConfig.CLIENT_ID_CONFIG, properties.getProperty("clientIdConfig"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        topicName = properties.getProperty("topicName");
        createKafkaConfig(props);
        this.produce = produce;
    }

    public void createKafkaConfig(Map<String, Object> props) {
        SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);
        reactiveKafkaProducerTemplate = new ReactiveKafkaProducerTemplate(senderOptions);
    }

    @Override
    public void generateErrorMessage(String errorRecord) {
        produce.sendMessages(errorRecord, topicName, reactiveKafkaProducerTemplate);
    }

    @Override
    public void stop() {
        reactiveKafkaProducerTemplate.close();
    }
}
