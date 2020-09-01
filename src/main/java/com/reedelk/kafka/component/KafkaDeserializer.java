package com.reedelk.kafka.component;

import com.reedelk.runtime.api.annotation.DisplayName;
import org.apache.kafka.common.serialization.*;

public enum KafkaDeserializer {

    @DisplayName("Byte Array")
    BYTE_ARRAY {
        @Override
        public Deserializer<?> create() {
            return new ByteArrayDeserializer();
        }
    },

    @DisplayName("Double")
    DOUBLE {
        @Override
        public Deserializer<?> create() {
            return new DoubleDeserializer();
        }
    },

    @DisplayName("Float")
    FLOAT {
        @Override
        public Deserializer<?> create() {
            return new FloatDeserializer();
        }
    },

    @DisplayName("Integer")
    INTEGER {
        @Override
        public Deserializer<?> create() {
            return new IntegerDeserializer();
        }
    },

    @DisplayName("Long")
    LONG {
        @Override
        public Deserializer<?> create() {
            return new LongDeserializer();
        }
    },

    @DisplayName("Short")
    SHORT {
        @Override
        public Deserializer<?> create() {
            return new ShortDeserializer();
        }
    },

    @DisplayName("String")
    STRING {
        @Override
        public Deserializer<?> create() {
            return new StringDeserializer();
        }
    },

    @DisplayName("UUID")
    UUID {
        @Override
        public Deserializer<?> create() {
            return new UUIDDeserializer();
        }
    },

    @DisplayName("Void")
    VOID {
        @Override
        public Deserializer<?> create() {
            return new VoidDeserializer();
        }
    };

    public abstract Deserializer<?> create();
}
