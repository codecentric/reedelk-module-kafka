package com.reedelk.kafka.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.Map;

import static com.reedelk.kafka.internal.attribute.KafkaProducerAttributes.*;

@Type
@TypeProperty(name = TIMESTAMP, type = long.class)
@TypeProperty(name = PARTITION, type = int.class)
@TypeProperty(name = TOPIC, type = String.class)
@TypeProperty(name = SUCCESS, type = boolean.class)
@TypeProperty(name = RECORD, type = Map.class)
public class KafkaProducerAttributes extends MessageAttributes {

    static final String RECORD = "record";
    static final String SUCCESS = "success";
    static final String TIMESTAMP = "timestamp";
    static final String PARTITION = "partition";
    static final String TOPIC = "topic";

    public KafkaProducerAttributes(RecordMetadata metadata) {
        put(SUCCESS, true);
        put(TOPIC, metadata.topic());
        put(PARTITION, metadata.partition());
        put(TIMESTAMP, metadata.timestamp());
    }

    public KafkaProducerAttributes(HashMap<Object, Object> unsentRecord) {
        put(SUCCESS, false);
        put(RECORD, unsentRecord);
    }
}