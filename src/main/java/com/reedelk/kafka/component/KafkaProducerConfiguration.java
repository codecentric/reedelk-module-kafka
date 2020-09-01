package com.reedelk.kafka.component;

import com.reedelk.runtime.api.annotation.*;
import com.reedelk.runtime.api.component.Implementor;
import org.osgi.service.component.annotations.Component;

import static org.osgi.service.component.annotations.ServiceScope.PROTOTYPE;

@Shared
@Component(service = KafkaProducerConfiguration.class, scope = PROTOTYPE)
public class KafkaProducerConfiguration implements Implementor {

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

    @Property("Client Id")
    @Hint("client1")
    @Example("client1")
    @Description("An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.")
    private String clientId;

    @Property("Key Serializer")
    @InitValue("STRING")
    @DefaultValue("STRING")
    @Description("The deserializer type to be used for the Kafka record key")
    private KafkaSerializer keySerializer;

    @Property("Value Serializer")
    @InitValue("STRING")
    @DefaultValue("STRING")
    @Description("The deserializer type to be used for the Kafka record value")
    private KafkaSerializer valueSerializer;


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }


    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public KafkaSerializer getKeySerializer() {
        return keySerializer;
    }

    public void setKeySerializer(KafkaSerializer keySerializer) {
        this.keySerializer = keySerializer;
    }

    public KafkaSerializer getValueSerializer() {
        return valueSerializer;
    }

    public void setValueSerializer(KafkaSerializer valueSerializer) {
        this.valueSerializer = valueSerializer;
    }
}
