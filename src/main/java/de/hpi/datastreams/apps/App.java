package de.hpi.datastreams.apps;

import de.hpi.datastreams.messages.SerializableHashMap;
import de.hpi.datastreams.processors.PredictionProcessor;
import de.hpi.datastreams.processors.ServerProcessor;
import de.hpi.datastreams.processors.WorkerSamplingProcessor;
import de.hpi.datastreams.processors.WorkerTrainingProcessor;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class App extends BaseKafkaApp {

    public final static int numWorkers = 4;

    public final static String INPUT_DATA_TOPIC = "INPUT_DATA";
    public final static Integer INPUT_DATA_NUM_PARTITIONS = numWorkers;
    public final static String INPUT_DATA_BUFFER = "INPUT_DATA_BUFFER";

    public final static String GRADIENTS_TOPIC = "GRADIENTS_TOPIC";
    public final static String WEIGHTS_TOPIC = "WEIGHTS_TOPIC";
    public final static Integer WEIGHTS_TOPIC_NUM_PARTITIONS = numWorkers;

    public final static String PREDICTION_DATA_TOPIC = "PREDICTION_DATA_TOPIC";
    public final static String PREDICTION_OUTPUT_TOPIC = "PREDICTION_OUTPUT_TOPIC";
    public final static String WEIGHTS_STORE = "WEIGHTS_STORE";

    // A vector clock of -1 identifies messages that are used to wake up the training loop.
    // Regular training iterations (vector clocks) are counted from 0 upwards.
    public static Integer START_VC = -1;

    private long bufferSize;

    public App() {
        this.bufferSize = 1000;
    }

    public App(int bufferSize) {
        this.bufferSize = bufferSize;

        try {
            this.createTopics();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        Logger.getLogger("org").setLevel(Level.OFF);
    }

    private void createTopics() throws IOException {
        AdminClient adminClient = AdminClient.create(this.getProperties());

        List<NewTopic> newTopics = new ArrayList<>();
        // new NewTopic(topicName, numPartitions, replicationFactor)
        newTopics.add(new NewTopic(INPUT_DATA_TOPIC, INPUT_DATA_NUM_PARTITIONS, (short) 1));
        newTopics.add(new NewTopic(WEIGHTS_TOPIC, WEIGHTS_TOPIC_NUM_PARTITIONS, (short) 1));
        newTopics.add(new NewTopic(GRADIENTS_TOPIC, 1, (short) 1));
        newTopics.add(new NewTopic(PREDICTION_DATA_TOPIC, 1, (short) 1));
        newTopics.add(new NewTopic(PREDICTION_OUTPUT_TOPIC, 1, (short) 1));

        adminClient.createTopics(newTopics);
        adminClient.close();
    }

    @Override
    public Topology getTopology(Properties properties) {

        return new Topology()
                .addSource("data-source", INPUT_DATA_TOPIC)
                .addProcessor("SamplingProcessor", () -> new WorkerSamplingProcessor(this.bufferSize), "data-source")

//                .addSource("prediction-source", PREDICTION_DATA_TOPIC)
//                .addProcessor("PredictionProcessor", PredictionProcessor::new, "prediction-source")

                .addSource("gradients-source", GRADIENTS_TOPIC)
                .addProcessor("ServerProcessor", ServerProcessor::new, "gradients-source")

                .addSource("weights-source", WEIGHTS_TOPIC)
                .addProcessor("TrainingProcessor", WorkerTrainingProcessor::new, "weights-source")


                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(INPUT_DATA_BUFFER),
                        Serdes.Long(), new JSONSerde<>()), "SamplingProcessor", "TrainingProcessor")
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(WEIGHTS_STORE),
                        Serdes.Long(), new JSONSerde<SerializableHashMap>()), "ServerProcessor");//, "PredictionProcessor");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "parameter-server";
    }

}

