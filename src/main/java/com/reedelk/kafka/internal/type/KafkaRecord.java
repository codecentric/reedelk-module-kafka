package com.reedelk.kafka.internal.type;

import com.reedelk.runtime.api.annotation.Type;
import com.reedelk.runtime.api.annotation.TypeProperty;

import java.util.HashMap;

@Type(mapKeyType = Object.class, mapValueType = Object.class)
@TypeProperty(name = KafkaRecord.KEY, type = Object.class)
@TypeProperty(name = KafkaRecord.VALUE, type = Object.class)
public class KafkaRecord extends HashMap<String, Object> {

    public static final String KEY = "key";
    public static final String VALUE = "value";

    public KafkaRecord(Object key, Object value) {
        put(KEY, key);
        put(VALUE, value);
    }
}
