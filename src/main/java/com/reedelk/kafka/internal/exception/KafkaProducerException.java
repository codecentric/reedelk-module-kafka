package com.reedelk.kafka.internal.exception;

import com.reedelk.runtime.api.exception.PlatformException;

public class KafkaProducerException extends PlatformException {

    public KafkaProducerException(String message) {
        super(message);
    }

    public KafkaProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}
