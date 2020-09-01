package com.reedelk.kafka.internal;

import com.reedelk.kafka.component.KafkaConsumerConfiguration;
import com.reedelk.kafka.component.KafkaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Optional;
import java.util.Properties;

public class KafkaConsumerFactory {

    public static KafkaConsumer<?, ?> from(KafkaConsumerConfiguration configuration) {
        Properties kafkaProperties = new Properties();

        String bootstrapServers = Optional.ofNullable(configuration.getBootstrapServers()).orElse(Defaults.BOOTSTRAP_SERVERS);
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String groupId = Optional.ofNullable(configuration.getGroupId()).orElse(Defaults.GROUP_ID);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        boolean enableAutoCommit = Optional.ofNullable(configuration.getEnableAutoCommit()).orElse(Defaults.ENABLE_AUTO_COMMIT);
        kafkaProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));

        int autoCommitInterval = Optional.ofNullable(configuration.getAutoCommitInterval()).orElse(Defaults.AUTO_COMMIT_INTERVAL);
        kafkaProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitInterval));

        KafkaDeserializer keyDeserializer = configuration.getKeyDeserializer();
        KafkaDeserializer valueDeserializer = configuration.getValueDeserializer();

        // We must temporarily use a different classloader otherwise the serializer
        // classes will belong to a different classloader and it won't be possible
        // to create the kafka consumer.
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(kafkaProperties, keyDeserializer.create(), valueDeserializer.create());
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        return consumer;
    }
}
