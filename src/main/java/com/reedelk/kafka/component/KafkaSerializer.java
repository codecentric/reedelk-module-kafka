package com.reedelk.kafka.component;

import com.reedelk.runtime.api.annotation.DisplayName;
import org.apache.kafka.common.serialization.*;

public enum KafkaSerializer {

    @DisplayName("Byte Array")
    BYTE_ARRAY {
        @Override
        public Serializer<?> create() {
            return new ByteArraySerializer();
        }
    },

    @DisplayName("Double")
    DOUBLE {
        @Override
        public Serializer<?> create() {
            return new DoubleSerializer();
        }
    },

    @DisplayName("Float")
    FLOAT {
        @Override
        public Serializer<?> create() {
            return new FloatSerializer();
        }
    },

    @DisplayName("Integer")
    INTEGER {
        @Override
        public Serializer<?> create() {
            return new IntegerSerializer();
        }
    },

    @DisplayName("Long")
    LONG {
        @Override
        public Serializer<?> create() {
            return new LongSerializer();
        }
    },

    @DisplayName("Short")
    SHORT {
        @Override
        public Serializer<?> create() {
            return new ShortSerializer();
        }
    },

    @DisplayName("String")
    STRING {
        @Override
        public Serializer<?> create() {
            return new StringSerializer();
        }
    },

    @DisplayName("UUID")
    UUID {
        @Override
        public Serializer<?> create() {
            return new UUIDSerializer();
        }
    },

    @DisplayName("Void")
    VOID {
        @Override
        public Serializer<?> create() {
            return new VoidSerializer();
        }
    };

    public abstract Serializer<?> create();
}
