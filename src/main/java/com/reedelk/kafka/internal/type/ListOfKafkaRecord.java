package com.reedelk.kafka.internal.type;

import com.reedelk.runtime.api.annotation.Type;

import java.util.ArrayList;

@Type(listItemType = KafkaRecord.class)
public class ListOfKafkaRecord extends ArrayList<KafkaRecord> {
}
