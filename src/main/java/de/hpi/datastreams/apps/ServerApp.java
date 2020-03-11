package de.hpi.datastreams.apps;

import de.hpi.datastreams.messages.LabeledData;
import de.hpi.datastreams.messages.SerializableHashMap;
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

import java.util.*;
import java.util.concurrent.ExecutionException;

public class ServerApp extends BaseKafkaApp {

    public ServerApp() {
        try {
            this.createTopics();
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        Logger.getLogger("org").setLevel(Level.OFF);
    }

    private void createTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(this.getProperties());

        List<NewTopic> newTopics = new ArrayList<>();
        // new NewTopic(topicName, numPartitions, replicationFactor)
        newTopics.add(new NewTopic(INPUT_DATA_TOPIC, INPUT_DATA_NUM_PARTITIONS, (short) 1));
        newTopics.add(new NewTopic(WEIGHTS_TOPIC, WEIGHTS_TOPIC_NUM_PARTITIONS, (short) 1));
        newTopics.add(new NewTopic(GRADIENTS_TOPIC, 1, (short) 1));
        newTopics.add(new NewTopic(PREDICTION_DATA_TOPIC, 1, (short) 1));
        newTopics.add(new NewTopic(PREDICTION_OUTPUT_TOPIC, 1, (short) 1));

        /*
        for(NewTopic t: newTopics){
            if (!adminClient.listTopics().names().get().contains(t.name())){
                System.out.println("Creating topic " + t.name());
                adminClient.createTopics(Collections.singleton(t));
            }
        }
         */

        adminClient.createTopics(newTopics);
        adminClient.close();
    }

    @Override
    public Topology getTopology(Properties properties) {

        return new Topology()
                .addSource("gradients-source", GRADIENTS_TOPIC)
                .addProcessor("ServerProcessor", ServerProcessor::new, "gradients-source")

                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(WEIGHTS_STORE),
                        Serdes.Long(), new JSONSerde<SerializableHashMap>()), "ServerProcessor");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "parameter-server-server";
    }

}

