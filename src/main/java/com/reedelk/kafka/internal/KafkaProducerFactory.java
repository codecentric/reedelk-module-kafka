package com.reedelk.kafka.internal;

import com.reedelk.kafka.component.KafkaProducerConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;

public class KafkaProducerFactory {

    public static KafkaProducer<String, String> from(KafkaProducerConfiguration configuration) {
        Properties kafkaProperties = new Properties();

        String bootstrapServers = Optional.ofNullable(configuration.getBootstrapServers()).orElse(Defaults.BOOTSTRAP_SERVERS);
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String clientId = Optional.ofNullable(configuration.getClientId()).orElse(Defaults.CLIENT_ID);
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);


        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Uses the wrong class
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        KafkaProducer<String, String> producer =
                new KafkaProducer<>(kafkaProperties, new StringSerializer(), new StringSerializer());
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        return producer;
    }
}
