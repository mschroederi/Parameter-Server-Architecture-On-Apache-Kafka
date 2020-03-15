package de.hpi.datastreams.apps;

import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Properties;
import java.util.concurrent.Callable;

import static org.apache.kafka.common.requests.DeleteAclsResponse.log;

public abstract class BaseKafkaApp implements Callable<Void> {

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


    final static String TRAINING_DATA_FILE_PATH = "./data/train.csv";
    final static String TRAINING_DATA_DEFAULT_URL = "https://s3.eu-central-1.amazonaws.com/de.hpi.datastreams.parameter-server/reviews-embedded_equal-distribution_train.csv";
    final static String TEST_DATA_FILE_PATH = "./data/test.csv";
    final static String TEST_DATA_DEFAULT_URL = "https://s3.eu-central-1.amazonaws.com/de.hpi.datastreams.parameter-server/reviews-embedded_equal-distribution_test.csv";

    private String host = "localhost";
    private int port = 8070;
    public static String brokers = "localhost:29092";  // this might be changed, depending on the arguments

    @Override
    public Void call() {
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

    protected Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, this.APPLICATION_ID_CONFIG());
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, String.format("%s:%s", this.host, this.port));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class.getName());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
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

    protected enum DATASET {
        TRAIN,
        TEST
    }

    static void download(DATASET dataset, String url) throws IOException {
        String fileName = "";
        switch (dataset) {
            case TRAIN:
                fileName = "train.csv";
                break;
            case TEST:
                fileName = "test.csv";
                break;
        }

        InputStream in = new URL(url).openStream();
        Files.copy(in, Paths.get("./data/" + fileName), StandardCopyOption.REPLACE_EXISTING);
    }

    static void downloadDatasetsIfNecessary(String trainingDataUrl, String testDataUrl) throws IOException {
        // Check whether the CSV files containing the testing & training data exists
        // If not, download them into the expected file within the data folder
        File trainingData = new File("./data/train.csv");
        File testingData = new File("./data/test.csv");
        new File("./data").mkdirs();
        if (!trainingData.exists()) BaseKafkaApp.download(BaseKafkaApp.DATASET.TRAIN, trainingDataUrl);
        if (!testingData.exists()) BaseKafkaApp.download(BaseKafkaApp.DATASET.TEST, testDataUrl);
    }
}
