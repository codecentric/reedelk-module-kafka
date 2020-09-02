package com.reedelk.kafka.internal.attribute;

import com.reedelk.kafka.internal.KafkaRecordMetadata;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;
import com.reedelk.runtime.api.message.MessageAttributes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Type
@TypeProperty(name = KafkaProducerAttributesList.METADATA, type = List.class)
public class KafkaProducerAttributesList extends MessageAttributes {

    static final String METADATA = "metadata";

    public KafkaProducerAttributesList(List<KafkaRecordMetadata> metadata) {
        ArrayList<KafkaProducerAttributes> attributes = new ArrayList<>();
        metadata.forEach(kafkaRecordMetadata -> {
            KafkaProducerAttributes attribute;
            if (kafkaRecordMetadata.success) {
                attribute = new KafkaProducerAttributes(kafkaRecordMetadata.recordMetadata);
            } else {
                HashMap<Object, Object> recordAsHashMap = new HashMap<>();
                recordAsHashMap.put(KafkaRecord.KEY, kafkaRecordMetadata.unsentRecord.get(KafkaRecord.KEY));
                recordAsHashMap.put(KafkaRecord.VALUE, kafkaRecordMetadata.unsentRecord.get(KafkaRecord.VALUE));
                attribute = new KafkaProducerAttributes(recordAsHashMap);
            }
            attributes.add(attribute);
        });

        put(METADATA, attributes);
    }
}
