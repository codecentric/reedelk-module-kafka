package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaProducerFactory;
import com.reedelk.kafka.internal.KafkaProducerHandler;
import com.reedelk.kafka.internal.attribute.KafkaProducerAttributes;
import com.reedelk.kafka.internal.attribute.KafkaProducerAttributesList;
import com.reedelk.kafka.internal.exception.KafkaProducerException;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.kafka.internal.type.ListOfKafkaRecord;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.commons.ComponentPrecondition.Input;
import com.reedelk.runtime.api.component.ProcessorSync;
import com.reedelk.runtime.api.flow.FlowContext;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.osgi.service.component.annotations.Component;

import java.util.List;
import java.util.Map;

import static com.reedelk.kafka.internal.commons.Messages.KafkaProducer.UNEXPECTED_INPUT;
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
    @Property("Configuration")
    private KafkaProducerConfiguration configuration;

    @Property("Topic")
    @Example("payments")
    @Hint("payments")
    @Description("The topic the records sent by this producer will be appended to.")
    private String topic;

    @Override
    public void initialize() {
        requireNotBlank(KafkaProducer.class, topic, "Topic must be set for Kafka producer component");
        requireNotNull(KafkaProducer.class, configuration, "Kafka Producer Configuration must be defined");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Message apply(FlowContext flowContext, Message message) {

        try (org.apache.kafka.clients.producer.KafkaProducer<?, ?> producer =
                     KafkaProducerFactory.from(configuration)) {

            Object payload = message.payload();

            Input.requireTypeMatchesAny(KafkaProducer.class, payload, List.class, Map.class);

            KafkaProducerHandler handler = new KafkaProducerHandler(topic);

            // If map, we send a single record.
            if (payload instanceof Map) {
                RecordMetadata recordMetadata = handler.send(producer, (Map<Object, Object>) payload);
                return MessageBuilder.get(KafkaProducer.class)
                        .withJavaObject(payload)
                        .attributes(new KafkaProducerAttributes(recordMetadata))
                        .build();

            } else if (payload instanceof List) {
                List<Object> recordsList = (List<Object>) payload;
                KafkaProducerHandler.RecordsSentResult result = handler.send(producer, recordsList);
                return MessageBuilder.get(KafkaProducer.class)
                        .withJavaObject(payload)
                        .attributes(new KafkaProducerAttributesList(result))
                        .build();

            } else {
                String error = UNEXPECTED_INPUT.format();
                throw new KafkaProducerException(error);
            }
        }
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
