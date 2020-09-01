package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaProducerFactory;
import com.reedelk.kafka.internal.attribute.KafkaProducerAttributes;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.exception.PlatformException;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageAttributes;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.osgi.service.component.annotations.Component;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@ModuleComponent("KafkaTopicProducer")
@ComponentOutput(attributes = MessageAttributes.class, payload = Object.class, description = "KafkaTopicProducer Output description")
@ComponentInput(payload = Object.class, description = "KafkaTopicProducer Input description")
@Description("KafkaTopicProducer description")
@Component(service = KafkaProducer.class, scope = PROTOTYPE)
public class KafkaProducer implements ProcessorSync {

    @DialogTitle("Kafka Producer Configuration")
    @Property("Connection")
    private KafkaProducerConfiguration configuration;

    @TabGroup("Topic")
    @Property("Producer Topics")
    @Description("List of topics to subscribe for")
    private String topic;

    @Override
    public void initialize() {

    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public Message apply(FlowContext flowContext, Message message) {
        // Input is a map key and values
        try (org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer =
                     KafkaProducerFactory.from(configuration)) {

            // TODO: We might also accept lists here
            Map<String, String> record = message.payload();
            String recordKey = record.get(KafkaRecord.KEY);
            String recordValue = record.get(KafkaRecord.VALUE);

            ProducerRecord producerRecord = new ProducerRecord<>(topic, recordKey, recordValue);
            Future<RecordMetadata> send = producer.send(producerRecord);
            RecordMetadata recordMetadata = send.get();

            return MessageBuilder.get(KafkaProducer.class)
                    .withJavaObject(message.payload())
                    .attributes(new KafkaProducerAttributes(recordMetadata))
                    .build();

        } catch (InterruptedException | ExecutionException e) {
            throw new PlatformException(e);
        }
    }

    @Override
    public void dispose() {
        // dispose logic
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
