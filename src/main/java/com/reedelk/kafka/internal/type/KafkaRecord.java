package com.reedelk.kafka.internal.type;

import com.reedelk.runtime.api.annotation.Type;

import java.util.HashMap;

@Type(mapKeyType = String.class, mapValueType = Object.class)
public class KafkaRecord extends HashMap<String, Object> {

    public static final String KEY = "key";
    public static final String VALUE = "value";

    public KafkaRecord(Object key, Object value) {
        put(KEY, key);
        put(VALUE, value);
    }
}
