package com.reedelk.kafka.internal;

import com.reedelk.kafka.component.ConnectionConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Optional;
import java.util.Properties;

public class KafkaConsumerFactory {

    public static KafkaConsumer<String,String> from(ConnectionConfiguration configuration) {
        Properties kafkaProperties = new Properties();

        String bootstrapServers = Optional.ofNullable(configuration.getBootstrapServers()).orElse(Defaults.BOOTSTRAP_SERVERS);
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        String groupId = Optional.ofNullable(configuration.getGroupId()).orElse(Defaults.GROUP_ID);
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        boolean enableAutoCommit = Optional.ofNullable(configuration.getEnableAutoCommit()).orElse(Defaults.ENABLE_AUTO_COMMIT);
        kafkaProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.toString(enableAutoCommit));

        int autoCommitInterval = Optional.ofNullable(configuration.getAutoCommitInterval()).orElse(Defaults.AUTO_COMMIT_INTERVAL);
        kafkaProperties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, String.valueOf(autoCommitInterval));

        // Uses the wrong class
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProperties, new StringDeserializer(), new StringDeserializer());
        Thread.currentThread().setContextClassLoader(contextClassLoader);
        return consumer;
    }
}
