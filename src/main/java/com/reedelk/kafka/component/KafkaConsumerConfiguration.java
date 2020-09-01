package com.reedelk.kafka.component;

import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;

import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@Shared
@Component(service = KafkaConsumerConfiguration.class, scope = PROTOTYPE)
public class KafkaConsumerConfiguration implements Implementor {

    @Property("Bootstrap Servers")
    @Hint("localhost:9092")
    @Example("localhost:9092")
    @DefaultValue("localhost:9092")
    @InitValue("localhost:9092")
    @Description("\"A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form \"\n" +
            "                                                       + \"<code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to \"\n" +
            "                                                       + \"discover the full cluster membership (which may change dynamically), this list need not contain the full set of \"\n" +
            "                                                       + \"servers (you may want more than one, though, in case a server is down).\"")
    private String bootstrapServers;

    @Property("Group ID")
    @Hint("test")
    @Example("test")
    @DefaultValue("test")
    @Description("A unique string that identifies the consumer group this consumer belongs to. This property is required if the consumer uses either the group management functionality by using <code>subscribe(topic)</code> or the Kafka-based offset management strategy.")
    private String groupId;

    @Property("Key Deserializer")
    @InitValue("STRING")
    @DefaultValue("STRING")
    @Description("The deserializer type to be used for the Kafka record key")
    private KafkaDeserializer keyDeserializer;

    @Property("Value Deserializer")
    @InitValue("STRING")
    @DefaultValue("STRING")
    @Description("The deserializer type to be used for the Kafka record value")
    private KafkaDeserializer valueDeserializer;

    @Property("Enable Auto Commit")
    @Example("false")
    @DefaultValue("false")
    @InitValue("true")
    @Description("If true the consumer's offset will be periodically committed in the background.")
    private Boolean enableAutoCommit;

    @Property("Auto Commit Interval (ms)")
    @Hint("1000")
    @DefaultValue("1000")
    @Example("1500")
    @InitValue("1000")
    @Description("The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if <code>enable.auto.commit</code> is set to <code>true</code>.")
    private Integer autoCommitInterval;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Boolean getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public void setEnableAutoCommit(Boolean enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public Integer getAutoCommitInterval() {
        return autoCommitInterval;
    }

    public void setAutoCommitInterval(Integer autoCommitInterval) {
        this.autoCommitInterval = autoCommitInterval;
    }

    public KafkaDeserializer getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(KafkaDeserializer keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public KafkaDeserializer getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(KafkaDeserializer valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }
}
