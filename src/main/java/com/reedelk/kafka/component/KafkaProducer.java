package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaProducerFactory;
import com.reedelk.kafka.internal.KafkaRecordMetadata;
import com.reedelk.kafka.internal.attribute.KafkaProducerAttributes;
import com.reedelk.kafka.internal.attribute.KafkaProducerAttributesList;
import com.reedelk.kafka.internal.commons.Messages;
import com.reedelk.kafka.internal.exception.KafkaProducerException;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.kafka.internal.type.ListOfKafkaRecord;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ComponentPrecondition;
import com.reedelk.runtime.api.commons.ComponentPrecondition.Input;
import com.reedelk.runtime.api.commons.Preconditions;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.osgi.service.component.annotations.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.reedelk.kafka.internal.commons.Messages.KafkaProducer.*;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotNull;
import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@ModuleComponent("Kafka Producer")
@ComponentOutput(
        attributes = { KafkaProducerAttributes.class, KafkaProducerAttributesList.class },
        payload = ComponentOutput.PreviousComponent.class,
        description = "The Kafka Producer component output is the original input message. " +
                "The payload is not changed by this component.")
@ComponentInput(
        payload = { KafkaRecord.class, ListOfKafkaRecord.class },
        description = "If the component input is a map, then it must contain a 'key' property and a 'value' property defining the kafka record to be sent. " +
                "If the component input is a list, the list must contain map objects. " +
                "The map objects in the list must contain a 'key' property and a 'value' property defining the kafka record to be sent.")
@Description("Sends a single record or multiple records to a Kafka topic. " +
        "If the component input is a map, then it <b>must</b> contain a 'key' property and a 'value' property defining the kafka record to be sent. If the map does not contain a key and a value property an exception will be thrown. " +
        "The type of the key and of the value must be consistent with the key and value serializers chosen in the Producer Configuration. " +
        "If the component input is a list, the list must contain map objects. The map objects in the list <b>must</b> contain a 'key' property and a 'value' property defining the kafka record to be sent. " +
        "If the map does not contain a key and a value property an exception will be thrown. " +
        "The topic and producer configuration properties are mandatory in order to use the Kafka Producer component.")
@Component(service = KafkaProducer.class, scope = PROTOTYPE)
public class KafkaProducer implements ProcessorSync {

    @DialogTitle("Kafka Producer Configuration")
    @Property("Producer Configuration")
    private KafkaProducerConfiguration configuration;

    @Property("Producer Topic")
    @Description("Producer topic")
    private String topic;

    @Override
    public void initialize() {
        requireNotBlank(KafkaProducer.class, topic, "Topic must be set for Kafka producer component");
        requireNotNull(KafkaProducer.class, configuration, "Kafka Producer Configuration must be defined");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Message apply(FlowContext flowContext, Message message) {
        // Input is a map key and values
        try (org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer =
                     KafkaProducerFactory.from(configuration)) {

            Object payload = message.payload();

            Input.requireTypeMatchesAny(KafkaProducer.class, payload, List.class, Map.class);

            // If map, we send a single record.
            if (payload instanceof Map) {
                RecordMetadata recordMetadata = handleMap(producer, (Map<Object, Object>) payload);
                return MessageBuilder.get(KafkaProducer.class)
                        .withJavaObject(payload)
                        .attributes(new KafkaProducerAttributes(recordMetadata))
                        .build();

            } else if (payload instanceof List) {
                List<Object> recordsList = (List<Object>) payload;
                List<KafkaRecordMetadata> recordMetadataList = handleList(producer, recordsList);
                return MessageBuilder.get(KafkaProducer.class)
                        .withJavaObject(recordMetadataList)
                        .attributes(new KafkaProducerAttributesList(recordMetadataList))
                        .build();

            } else {
                String error = UNEXPECTED_INPUT.format();
                throw new KafkaProducerException(error);
            }
        }
    }

    @SuppressWarnings({"unchecked"})
    private List<KafkaRecordMetadata> handleList(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, List<Object> recordList) {
        List<FutureRequest> futures = new ArrayList<>();
        for (Object item : recordList) {
            Input.requireTypeMatches(KafkaProducer.class, item, Map.class);

            Map<Object, Object> kafkaRecord = (Map<Object, Object>) item;
            Future<RecordMetadata> future = send(producer, kafkaRecord);
            futures.add(new FutureRequest(future, kafkaRecord));
        }

        List<KafkaRecordMetadata> recordMetadataList = new ArrayList<>();
        for (FutureRequest futureRequest : futures) {
            try {
                RecordMetadata recordMetadata = futureRequest.future.get();
                recordMetadataList.add(new KafkaRecordMetadata(recordMetadata));
            } catch (Exception exception) {
                // The record could not be sent.
                recordMetadataList.add(new KafkaRecordMetadata(futureRequest.record));
            }
        }
        return recordMetadataList;
    }

    static class FutureRequest {

        Future<RecordMetadata> future;
        Map<Object, Object> record;

        FutureRequest(Future<RecordMetadata> future, Map<Object,Object> record) {
            this.future = future;
            this.record = record;
        }
    }

    private RecordMetadata handleMap(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Map<Object, Object> record)  {
        Future<RecordMetadata> future = send(producer, record);
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException exception) {
            String error = RECORD_SEND_ERROR.format(record, exception.getMessage());
            throw new KafkaProducerException(error, exception);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Future<RecordMetadata> send(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Map<Object, Object> record) {
        Preconditions.checkArgument(
                record.containsKey(KafkaRecord.KEY) && record.containsKey(KafkaRecord.VALUE),
                "Kafka input record is not valid. Please provide a map with 'key' and 'value' property.");

        Object recordKey = record.get(KafkaRecord.KEY);
        Object recordValue = record.get(KafkaRecord.VALUE);
        ProducerRecord producerRecord = new ProducerRecord<>(topic, recordKey, recordValue);
        return producer.send(producerRecord);
    }

    public KafkaProducerConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(KafkaProducerConfiguration configuration) {
        this.configuration = configuration;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
