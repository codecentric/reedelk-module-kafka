package com.reedelk.kafka.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class KafkaConsumerException extends PlatformException {

    public KafkaConsumerException(String message) {
        super(message);
    }

    public KafkaConsumerException(String message, Throwable cause) {
        super(message, cause);
    }
}
