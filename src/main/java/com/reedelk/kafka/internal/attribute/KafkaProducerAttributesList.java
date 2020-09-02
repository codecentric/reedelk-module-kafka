package com.reedelk.kafka.internal.attribute;

import com.reedelk.kafka.internal.KafkaProducerHandler;
import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.reedelk.kafka.internal.attribute.KafkaProducerAttributesList.*;
import static com.reedelk.kafka.internal.attribute.KafkaProducerAttributesList.METADATA;

@Type
@TypeProperty(name = METADATA, type = List.class)
@TypeProperty(name = SUCCESS, type = boolean.class)
public class KafkaProducerAttributesList extends MessageAttributes {

    static final String METADATA = "metadata";
    static final String SUCCESS = "success";

    public KafkaProducerAttributesList(KafkaProducerHandler.RecordsSentResult result) {
        ArrayList<KafkaProducerAttributes> attributes = new ArrayList<>();
        result.recordMetadataList.forEach(kafkaRecordMetadata -> {
            KafkaProducerAttributes attribute = kafkaRecordMetadata.success ?
                    new KafkaProducerAttributes(kafkaRecordMetadata.recordMetadata) :
                    new KafkaProducerAttributes(new HashMap<>(kafkaRecordMetadata.unsentRecord));
            attributes.add(attribute);
        });

        put(SUCCESS, result.success);
        put(METADATA, attributes);
    }
}
