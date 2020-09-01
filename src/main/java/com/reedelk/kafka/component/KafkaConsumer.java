package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaConsumerFactory;
import com.reedelk.kafka.internal.KafkaConsumerRunnable;
import com.reedelk.kafka.internal.attribute.KafkaConsumerAttributes;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.AbstractInbound;
import org.osgi.service.component.annotations.Component;

import java.util.List;

import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.*;
import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@ModuleComponent("Kafka Topic Consumer")
@ComponentOutput(attributes = KafkaConsumerAttributes.class, payload = String.class, description = "KafkaConsumer Output description")
@Description("Kafka Topic Consumer")
@Component(service = KafkaConsumer.class, scope = PROTOTYPE)
public class KafkaConsumer extends AbstractInbound {

    @DialogTitle("Kafka Consumer Configuration")
    @Property("Connection")
    private KafkaConsumerConfiguration configuration;

    @TabGroup("Topics")
    @Property("Consumer Subscription Topics")
    @Description("List of topics to subscribe for")
    private List<String> topics;

    @Property("Poll timeout (ms)")
    @Group("Advanced")
    @DefaultValue("100")
    @Hint("100")
    @Example("500")
    @Description("The maximum time to block before the next poll in milliseconds")
    private Integer pollTimeout;

    private KafkaConsumerRunnable kafkaConsumerRunnable;
    private Thread kafkaThread;

    @Override
    public void onStart() {
        requireNotNull(KafkaConsumer.class, configuration, "Kafka Connection must be defined");
        requireNotNull(KafkaConsumer.class, topics, "Topics must be defined");
        requireTrue(KafkaConsumer.class, !topics.isEmpty(), "At least one topic must be defined");

        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer =
                KafkaConsumerFactory.from(configuration);

        kafkaConsumerRunnable = new KafkaConsumerRunnable(this, consumer, topics, pollTimeout);
        kafkaThread = new Thread(kafkaConsumerRunnable);
        kafkaThread.start();
    }

    @Override
    public void onShutdown() {
        if (kafkaThread != null) {
            kafkaConsumerRunnable.terminate();
            try {
                kafkaThread.join();
            } catch (InterruptedException e) {
                // nothing we can do
            }
        }
    }

    public void setConfiguration(KafkaConsumerConfiguration configuration) {
        this.configuration = configuration;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public void setPollTimeout(int pollTimeout) {
        this.pollTimeout = pollTimeout;
    }
}
