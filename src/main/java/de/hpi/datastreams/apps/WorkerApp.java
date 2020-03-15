package de.hpi.datastreams.apps;

import de.hpi.datastreams.messages.LabeledData;
import de.hpi.datastreams.messages.SerializableHashMap;
import de.hpi.datastreams.processors.PredictionProcessor;
import de.hpi.datastreams.processors.ServerProcessor;
import de.hpi.datastreams.processors.WorkerSamplingProcessor;
import de.hpi.datastreams.processors.WorkerTrainingProcessor;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

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


    static void downloadTestDatasetIfNecessary(String testDataUrl) throws IOException {
        // Check whether the CSV files containing the testing & training data exists
        // If not, download them into the expected file within the data folder
        File testingData = new File("./data/test.csv");
        new File("./data").mkdirs();
        if (!testingData.exists()) BaseKafkaApp.download(BaseKafkaApp.DATASET.TEST, testDataUrl);
    }
}

