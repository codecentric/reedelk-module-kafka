package com.reedelk.kafka.internal.type;

import com.reedelk.runtime.api.annotation.Type;

import java.util.HashMap;

@Type(mapKeyType = String.class, mapValueType = Object.class)
public class KafkaRecord extends HashMap<String, Object> {

    public KafkaRecord(Object key, Object value) {
        put("key", key);
        put("value", value);
    }
}
