package com.reedelk.kafka.internal;

import com.reedelk.kafka.component.KafkaConsumer;
import com.reedelk.kafka.internal.attribute.KafkaTopicConsumerAttributes;
import com.reedelk.kafka.internal.type.KafkaRecord;
import com.reedelk.runtime.api.component.InboundEventListener;
import com.reedelk.runtime.api.message.Message;
import com.reedelk.runtime.api.message.MessageBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class KafkaConsumerRunnable implements Runnable {

    private final org.apache.kafka.clients.consumer.KafkaConsumer<?,?> consumer;
    private final InboundEventListener eventListener;
    private final List<String> topics;
    private volatile boolean running = true;
    private final int pollTimeout;

    public KafkaConsumerRunnable(InboundEventListener eventListener, org.apache.kafka.clients.consumer.KafkaConsumer<?,?> consumer, List<String> topics, Integer pollTimeout) {
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

                ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(pollTimeout));

                for (ConsumerRecord<?, ?> record : records) {

                    KafkaRecord kafkaRecord = new KafkaRecord(record.key(), record.value());

                    KafkaTopicConsumerAttributes attributes = new KafkaTopicConsumerAttributes(record);

                    Message eventMessage = MessageBuilder.get(KafkaConsumer.class)
                            .withJavaObject(kafkaRecord)
                            .attributes(attributes)
                            .build();

                    eventListener.onEvent(eventMessage);
                }
            } catch (Exception exception) {
                running = false;
            }
        }
    }

    public void terminate() {
        running = false;
    }
}