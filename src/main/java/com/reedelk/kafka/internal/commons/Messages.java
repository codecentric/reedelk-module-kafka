package com.reedelk.kafka.internal.commons;

import com.reedelk.runtime.api.commons.FormattedMessage;

public class Messages {

    private Messages() {
    }

    public enum KafkaProducer implements FormattedMessage {

        UNEXPECTED_INPUT("The expected input is not a list or a map."),
        RECORD_SEND_ERROR("Could not send record=[%s], cause=[%s].");

        private String message;

        KafkaProducer(String message) {
            this.message = message;
        }

        @Override
        public String template() {
            return message;
        }
    }
}
