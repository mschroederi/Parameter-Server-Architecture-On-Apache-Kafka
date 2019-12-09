package de.hpi.datastreams.apps;

import de.hpi.datastreams.processors.ServerProcessor;
import de.hpi.datastreams.processors.WorkerSamplingProcessor;
import de.hpi.datastreams.processors.WorkerTrainingProcessor;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class App extends BaseKafkaApp {

    public final static String INPUT_DATA_TOPIC = "INPUT_DATA";
    public final static String INPUT_DATA_BUFFER = "INPUT_DATA_BUFFER";

    public final static String GRADIENTS_TOPIC = "GRADIENTS_TOPIC";
    public final static String WEIGHTS_TOPIC = "WEIGHTS_TOPIC";

    // A vector clock of -1 identifies messages that are used to wake up the training loop.
    // Regular training iterations (vector clocks) are counted from 0 upwards.
    public static Integer START_VC = -1;

    private long bufferSize;

    public App() {
        this.bufferSize = 1000;
    }

    public App(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    @Override
    public Topology getTopology(Properties properties) {

        return new Topology()
                .addSource("Data-Source", INPUT_DATA_TOPIC)
                .addProcessor("SamplingProcessor", () -> new WorkerSamplingProcessor(this.bufferSize), "Data-Source")

                .addSource("gradients-source", GRADIENTS_TOPIC)
                .addProcessor("ServerProcessor", ServerProcessor::new, "gradients-source")

                .addSource("weights-source", WEIGHTS_TOPIC)
                .addProcessor("TrainingProcessor", WorkerTrainingProcessor::new, "weights-source")

                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(INPUT_DATA_BUFFER),
                        Serdes.Long(), new JSONSerde<>()), "SamplingProcessor", "TrainingProcessor");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "parameter-server";
    }

}
