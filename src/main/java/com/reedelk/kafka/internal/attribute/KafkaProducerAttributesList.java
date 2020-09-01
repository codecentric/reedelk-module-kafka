package com.reedelk.kafka.internal.attribute;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.List;

import static com.reedelk.kafka.internal.attribute.KafkaConsumerAttributes.*;

@Type
public class KafkaProducerAttributesList extends MessageAttributes {

    static final String METADATA = "metadata";

    public KafkaProducerAttributesList(List<RecordMetadata> metadatas) {
        // TODO: Complete me
    }
}
