package de.hpi.datastreams.apps;


import de.hpi.datastreams.processors.SamplingProcessor;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.Callable;

public class SamplingApp implements Callable<Void> {
    public final static String INPUT_DATA = "INPUT_DATA";
    public final static String INPUT_DATA_BUFFER = "INPUT_DATA_BUFFER";

    private long bufferSize;

    public SamplingApp() {
        this.bufferSize = 1000;
    }

    public SamplingApp(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public Topology getTopology(Properties properties) {

        return new Topology()
                .addSource("Edge-Source", INPUT_DATA)
                .addProcessor("EdgeProcessor", () -> new SamplingProcessor(this.bufferSize), "Edge-Source")
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(INPUT_DATA_BUFFER),
                        Serdes.Long(), new JSONSerde<>()), "EdgeProcessor");
    }

    @Override
    public Void call() throws Exception {
        final Properties properties = this.getProperties();
        final Topology topology = getTopology(properties);
        final KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.cleanUp();
        streams.start();

        return null;
    }

    public Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "parameter-server");
        //props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class.getName());

        return props;
    }
}

class SamplingAppRunner{
    public static void main(String[] args) {
        SamplingApp samplingApp = new SamplingApp(20);
        try {
            samplingApp.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
