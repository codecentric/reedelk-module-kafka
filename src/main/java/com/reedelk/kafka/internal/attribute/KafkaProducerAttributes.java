package com.reedelk.kafka.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;
import org.apache.kafka.clients.producer.RecordMetadata;

import static com.reedelk.kafka.internal.attribute.KafkaConsumerAttributes.*;

@Type
@TypeProperty(name = TIMESTAMP, type = long.class)
@TypeProperty(name = PARTITION, type = int.class)
@TypeProperty(name = TOPIC, type = String.class)
public class KafkaProducerAttributes extends MessageAttributes {

    static final String TIMESTAMP = "timestamp";
    static final String PARTITION = "partition";
    static final String TOPIC = "topic";

    public KafkaProducerAttributes(RecordMetadata metadata) {
        put(TOPIC, metadata.topic());
        put(PARTITION, metadata.partition());
        put(TIMESTAMP, metadata.timestamp());
    }
}