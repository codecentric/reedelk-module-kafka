package com.reedelk.kafka.internal.type;

import com.reedelk.runtime.api.annotation.Type;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

@Type(mapKeyType = String.class, mapValueType = Object.class)
public class KafkaRecord extends HashMap<String, Object> {

    public static final String KEY = "key";
    public static final String VALUE = "value";

    public KafkaRecord(Object key, Object value) {
        put(KEY, key);
        put(VALUE, value);
    }
}
