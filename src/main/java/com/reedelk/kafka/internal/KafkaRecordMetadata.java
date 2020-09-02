package com.reedelk.kafka.internal;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class KafkaRecordMetadata {

    public final boolean success;
    public final RecordMetadata recordMetadata;
    public final Map<Object,Object> unsentRecord;

    public KafkaRecordMetadata(RecordMetadata recordMetadata) {
        this.success = true;
        this.recordMetadata = recordMetadata;
        this.unsentRecord = null;
    }

    public KafkaRecordMetadata(Map<Object,Object> unsentRecord) {
        this.success = false;
        this.recordMetadata = null;
        this.unsentRecord = unsentRecord;
    }
}
