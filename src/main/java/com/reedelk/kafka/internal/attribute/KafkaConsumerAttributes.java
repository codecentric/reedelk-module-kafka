package com.reedelk.kafka.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import static com.reedelk.kafka.internal.attribute.KafkaConsumerAttributes.*;

@Type
@TypeProperty(name = KEY, type = String.class)
@TypeProperty(name = TOPIC, type = String.class)
@TypeProperty(name = OFFSET, type = long.class)
@TypeProperty(name = TIMESTAMP, type = long.class)
@TypeProperty(name = PARTITION, type = int.class)
public class KafkaConsumerAttributes extends MessageAttributes {

    static final String KEY = "key";
    static final String TOPIC = "topic";
    static final String OFFSET = "offset";
    static final String PARTITION = "envelope";
    static final String TIMESTAMP = "timestamp";

    public KafkaConsumerAttributes(ConsumerRecord<?, ?> record) {
        put(TOPIC, record.topic());
        put(OFFSET, record.offset());
        put(TIMESTAMP, record.timestamp());
        put(PARTITION, record.partition());
    }
}
