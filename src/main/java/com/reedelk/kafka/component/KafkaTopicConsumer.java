package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.KafkaConsumerFactory;
import com.reedelk.kafka.internal.attribute.KafkaTopicConsumerAttributes;
import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.AbstractInbound;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.osgi.service.component.annotations.Component;

import java.util.List;

import static com.reedelk.runtime.api.commons.ComponentPrecondition.Configuration.*;
import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@ModuleComponent("Kafka Topic Consumer")
@ComponentOutput(attributes = KafkaTopicConsumerAttributes.class, payload = String.class, description = "KafkaConsumer Output description")
@Description("Kafka Topic Consumer")
@Component(service = KafkaTopicConsumer.class, scope = PROTOTYPE)
public class KafkaTopicConsumer extends AbstractInbound {

    @DialogTitle("Kafka Connection Factory")
    @Property("Connection")
    private ConnectionConfiguration connection;

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

    private KafkaRunnable kafkaRunnable;
    private Thread kafkaThread;

    @Override
    public void onStart() {
        requireNotNull(KafkaTopicConsumer.class, connection, "Kafka Connection must be defined");
        requireNotNull(KafkaTopicConsumer.class, topics, "Topics must be defined");
        requireTrue(KafkaTopicConsumer.class, !topics.isEmpty(), "At least one topic must be defined");

        KafkaConsumer<String, String> consumer = KafkaConsumerFactory.from(connection);

        kafkaRunnable = new KafkaRunnable(this, consumer, topics, pollTimeout);
        kafkaThread = new Thread(kafkaRunnable);
        kafkaThread.start();
    }

    @Override
    public void onShutdown() {
        if (kafkaThread != null) {
            kafkaRunnable.terminate();
            try {
                kafkaThread.join();
            } catch (InterruptedException e) {
                // nothing we can do
            }
        }
    }

    public void setConnection(ConnectionConfiguration connection) {
        this.connection = connection;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public void setPollTimeout(int pollTimeout) {
        this.pollTimeout = pollTimeout;
    }
}
