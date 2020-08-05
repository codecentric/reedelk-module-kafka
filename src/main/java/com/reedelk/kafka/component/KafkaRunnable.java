package com.reedelk.kafka.component;

import com.reedelk.kafka.internal.attribute.KafkaTopicConsumerAttributes;
import com.reedelk.runtime.api.component.InboundEventListener;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import com.reedelk.runtime.api.message.content.MimeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class KafkaRunnable implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final InboundEventListener eventListener;
    private final List<String> topics;
    private volatile boolean running = true;
    private final int pollTimeout;

    KafkaRunnable(InboundEventListener eventListener, KafkaConsumer<String, String> consumer, List<String> topics, Integer pollTimeout) {
        this.eventListener = eventListener;
        this.consumer = consumer;
        this.topics = topics;
        this.pollTimeout = Optional.ofNullable(pollTimeout).orElse(100);
    }

    @Override
    public void run() {
        consumer.subscribe(topics);
        try {
            loop();
        } finally {
            try {
                consumer.close();
            } catch (Exception nothingWeCanDo) {
                // nothing we can do
                nothingWeCanDo.printStackTrace();
            }
        }
    }

    private void loop() {
        while (running) {

            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(pollTimeout));

                for (ConsumerRecord<String, String> record : records) {

                    String value = record.value();

                    KafkaTopicConsumerAttributes attributes = new KafkaTopicConsumerAttributes(record);

                    Message eventMessage = MessageBuilder.get(KafkaTopicConsumer.class)
                            .withString(value, MimeType.TEXT_PLAIN)
                            .attributes(attributes)
                            .build();

                    eventListener.onEvent(eventMessage);
                }
            } catch (Exception e) {
                running = false;
            }
        }
    }

    public void terminate() {
        running = false;
    }
}