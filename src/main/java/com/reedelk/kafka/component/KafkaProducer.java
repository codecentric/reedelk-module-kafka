package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaProducerFactory;
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

    @Override
    public Message apply(FlowContext flowContext, Message message) {
        // Input is a map key and values
        try (org.apache.kafka.clients.producer.KafkaProducer<String, String> producer =
                     KafkaProducerFactory.from(configuration)) {

            Map<String, String> record = message.payload();
            String recordKey = record.get("id");
            String recordValue = record.get("value");
            Future<RecordMetadata> send = producer.send(new ProducerRecord<>(topic, recordKey, recordValue));
            send.get();

            return MessageBuilder.get(KafkaProducer.class)
                    .empty()
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
