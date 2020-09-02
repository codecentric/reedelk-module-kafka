package com.reedelk.kafka.internal;

import com.reedelk.kafka.component.KafkaProducer;
import com.reedelk.kafka.internal.exception.KafkaProducerException;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.runtime.api.commons.ComponentPrecondition;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.reedelk.kafka.internal.commons.Messages.KafkaProducer.RECORD_SEND_ERROR;
import static com.reedelk.runtime.api.commons.Preconditions.*;

@SuppressWarnings({"rawtypes", "unchecked"})
public class KafkaProducerHandler {

    public static final Logger logger = LoggerFactory.getLogger(KafkaProducerHandler.class);

    private final String topic;

    public KafkaProducerHandler(String topic) {
        this.topic = topic;
    }

    public RecordMetadata send(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Map<Object, Object> record)  {
        Future<RecordMetadata> future = sendInternal(producer, record);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException exception) {
            String error = RECORD_SEND_ERROR.format(record, exception.getMessage());
            throw new KafkaProducerException(error, exception);
        }
    }

    public RecordsSentResult send(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, List<Object> recordList) {
        List<FutureRequest> futures = new ArrayList<>();
        for (Object item : recordList) {
            ComponentPrecondition.Input.requireTypeMatches(KafkaProducer.class, item, Map.class);

            Map<Object, Object> kafkaRecord = (Map<Object, Object>) item;
            Future<RecordMetadata> future = sendInternal(producer, kafkaRecord);
            futures.add(new FutureRequest(future, kafkaRecord));
        }

        List<KafkaRecordMetadata> recordMetadataList = new ArrayList<>();
        boolean success = true;
        for (FutureRequest futureRequest : futures) {
            try {
                RecordMetadata recordMetadata = futureRequest.future.get();
                recordMetadataList.add(new KafkaRecordMetadata(recordMetadata));
            } catch (Exception exception) {
                // An exception is not thrown. The user is responsible to check that all
                // the records have been sent correctly from the output message metadata.
                recordMetadataList.add(new KafkaRecordMetadata(futureRequest.record));
                String error = RECORD_SEND_ERROR.format(futureRequest.record, exception.getMessage());
                logger.warn(error);
                success = false;
            }
        }
        return new RecordsSentResult(recordMetadataList, success);
    }

    private Future<RecordMetadata> sendInternal(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Map<Object, Object> record) {
        checkArgument(
                record.containsKey(KafkaRecord.KEY) && record.containsKey(KafkaRecord.VALUE),
                "Kafka input record is not valid. Please provide a map with 'key' and 'value' property.");
        Object recordKey = record.get(KafkaRecord.KEY);
        Object recordValue = record.get(KafkaRecord.VALUE);
        ProducerRecord producerRecord = new ProducerRecord<>(topic, recordKey, recordValue);
        return producer.send(producerRecord);
    }

    public static class RecordsSentResult {

        public final List<KafkaRecordMetadata> recordMetadataList;
        public final boolean success;

        RecordsSentResult(List<KafkaRecordMetadata> recordMetadataList, boolean success) {
            this.recordMetadataList = recordMetadataList;
            this.success = success;
        }
    }

    static class FutureRequest {

        Future<RecordMetadata> future;
        Map<Object, Object> record;

        FutureRequest(Future<RecordMetadata> future, Map<Object,Object> record) {
            this.future = future;
            this.record = record;
        }
    }
}
