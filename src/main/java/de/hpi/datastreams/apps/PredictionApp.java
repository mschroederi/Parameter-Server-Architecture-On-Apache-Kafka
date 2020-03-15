package de.hpi.datastreams.apps;

import de.hpi.datastreams.messages.SerializableHashMap;
import de.hpi.datastreams.processors.PredictionProcessor;
import de.hpi.datastreams.processors.ServerProcessor;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PredictionApp extends BaseKafkaApp {

    private String testDataFilePath;

    public PredictionApp(String testDataFilePath){
        this.testDataFilePath = testDataFilePath;
    }

    public PredictionApp() {
        Logger.getLogger("org").setLevel(Level.OFF);
    }

    @Override
    public Topology getTopology(Properties properties) {

        return new Topology()
                .addSource("prediction-source", PREDICTION_DATA_TOPIC)
                .addProcessor("PredictionProcessor", () -> new PredictionProcessor(this.testDataFilePath), "prediction-source")

                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(WEIGHTS_STORE),
                        Serdes.Long(), new JSONSerde<SerializableHashMap>()),  "PredictionProcessor");
    }

    @Override
    public String APPLICATION_ID_CONFIG() {
        return "parameter-server-prediction";
    }

}

