package com.reedelk.kafka.internal;

import com.reedelk.kafka.component.KafkaProducerConfiguration;
import com.reedelk.kafka.component.KafkaSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;

public class KafkaProducerFactory {

    public static KafkaProducer<?, ?> from(KafkaProducerConfiguration configuration) {
        Properties kafkaProperties = new Properties();

        String bootstrapServers = Optional.ofNullable(configuration.getBootstrapServers()).orElse(Defaults.BOOTSTRAP_SERVERS);
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String clientId = Optional.ofNullable(configuration.getClientId()).orElse(Defaults.CLIENT_ID);
        kafkaProperties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);

        KafkaSerializer keySerializer = Optional.ofNullable(configuration.getKeySerializer()).orElse(KafkaSerializer.STRING);
        KafkaSerializer valueSerializer = Optional.ofNullable(configuration.getValueSerializer()).orElse(KafkaSerializer.STRING);

        // We must temporarily use a different classloader otherwise the serializer
        // classes will belong to a different classloader and it won't be possible
        // to create the kafka producer.
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);

        KafkaProducer<?, ?> producer =
                new KafkaProducer<>(kafkaProperties, keySerializer.create(), valueSerializer.create());
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        return producer;
    }
}
