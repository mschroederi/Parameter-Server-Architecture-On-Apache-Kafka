package de.hpi.datastreams.apps;

import de.hpi.datastreams.processors.WorkerSamplingProcessor;
import de.hpi.datastreams.processors.WorkerTrainingProcessor;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;

public class WorkerApp extends BaseKafkaApp {

    private int minBufferSize;
    private int maxBufferSize;
    private float bufferSizeCoefficient;
    private String testDataFile;

    public WorkerApp(int minBufferSize, int maxBufferSize, float bufferSizeCoefficient, String testDataFile) {
        this.minBufferSize = minBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.bufferSizeCoefficient = bufferSizeCoefficient;
        this.testDataFile = testDataFile;

        Logger.getLogger("org").setLevel(Level.OFF);
    }

    @Override
    public Topology getTopology(Properties properties) {

        return new Topology()
                .addSource("data-source", INPUT_DATA_TOPIC)
                .addProcessor("SamplingProcessor", () -> new WorkerSamplingProcessor(this.minBufferSize, this.maxBufferSize, this.bufferSizeCoefficient), "data-source")

                .addSource("weights-source", WEIGHTS_TOPIC)
                .addProcessor("TrainingProcessor", () -> new WorkerTrainingProcessor(this.maxBufferSize, this.testDataFile), "weights-source")

                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(INPUT_DATA_BUFFER),
                        Serdes.Long(), new JSONSerde<>()), "SamplingProcessor", "TrainingProcessor");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "parameter-server-worker";
    }

}

