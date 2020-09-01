package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaProducerFactory;
import com.reedelk.kafka.internal.attribute.KafkaProducerAttributes;
import com.reedelk.kafka.internal.exception.KafkaProducerException;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ComponentPrecondition.Input;
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

import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotBlank;
import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.requireNotNull;
import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@ModuleComponent("Kafka Producer")
@ComponentOutput(attributes = MessageAttributes.class, payload = Object.class, description = "KafkaTopicProducer Output description")
@ComponentInput(payload = Object.class, description = "KafkaTopicProducer Input description")
@Description("KafkaTopicProducer description")
@Component(service = KafkaProducer.class, scope = PROTOTYPE)
public class KafkaProducer implements ProcessorSync {

    @DialogTitle("Kafka Producer Configuration")
    @Property("Connection")
    private KafkaProducerConfiguration configuration;

    @Property("Producer Topic")
    @Description("Producer topic")
    private String topic;

    @Override
    public void initialize() {
        requireNotBlank(KafkaProducer.class, topic, "Topic must be set for Kafka producer component");
        requireNotNull(KafkaProducer.class, configuration, "Kafka Producer Configuration must be defined");
    }

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        // Input is a map key and values
        try (org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer =
                     KafkaProducerFactory.from(configuration)) {

            Object payload = message.payload();

            Input.requireTypeMatchesAny(KafkaProducer.class, payload, List.class, Map.class);

            if (payload instanceof Map) {
                return handleMap(producer, payload);
            } else if (payload instanceof List) {
                return handleList(producer, payload);
            } else {
                throw new IllegalStateException("");
            }

        } catch (InterruptedException | ExecutionException exception) {
            // TODO: Message
            String error = "error message";
            throw new KafkaProducerException(error, exception);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Message handleList(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Object payload) throws InterruptedException, ExecutionException {
        List payloadList = (List) payload;

        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (Object item : payloadList) {
            Input.requireTypeMatches(KafkaProducer.class, item, Map.class);
            Map<Object, Object> kafkaRecord = (Map<Object, Object>) item;
            Future<RecordMetadata> send = send(producer, kafkaRecord);
            futures.add(send);
        }

        List<RecordMetadata> recordMetadataList = new ArrayList<>();
        for (Future<RecordMetadata> future : futures) {
            try {
                RecordMetadata recordMetadata = future.get();
                recordMetadataList.add(recordMetadata);
            } catch (Exception exception) {
                // TODO: Handle me, add record metadata here
            }
        }

        return MessageBuilder.get(KafkaProducer.class)
                .withJavaObject(payload)
                .attributes(new KafkaProducerAttributes(recordMetadataList))
                .build();
    }

    @SuppressWarnings("unchecked")
    private Message handleMap(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Object payload) throws InterruptedException, ExecutionException {
        Future<RecordMetadata> recordMetadataFuture = send(producer, (Map<Object, Object>) payload);
        RecordMetadata recordMetadata = recordMetadataFuture.get();

        return MessageBuilder.get(KafkaProducer.class)
                .withJavaObject(payload)
                .attributes(new KafkaProducerAttributes(recordMetadata))
                .build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Future<RecordMetadata> send(org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer, Map<Object, Object> record) throws InterruptedException, ExecutionException {
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
