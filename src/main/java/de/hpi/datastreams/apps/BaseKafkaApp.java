package de.hpi.datastreams.apps;

import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import java.util.concurrent.Callable;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public abstract class BaseKafkaApp implements Callable<Void> {

    private String host = "localhost";
    private int port = 8070;
    public final static String brokers = "localhost:29092";

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = getTopology(properties);
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streams.close();
            } catch (Exception e) {
                log.warn("Error in shutdown", e);
            }
        }));

        streams.cleanUp();
        streams.start();

        return null;
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.APPLICATION_ID_CONFIG());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:%s", this.host, this.port));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Enable tracking message flow in Confluent Control Center
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        props.put(
                StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        return props;
    }

    public abstract Topology getTopology(Properties properties);

    public abstract String APPLICATION_ID_CONFIG();
}
