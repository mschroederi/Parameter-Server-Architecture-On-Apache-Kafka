package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.WorkerApp;
import de.hpi.datastreams.messages.*;
import de.hpi.datastreams.ml.LogisticRegressionTaskSpark;
import de.hpi.datastreams.ml.Metrics;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;

import static de.hpi.datastreams.apps.WorkerApp.GRADIENTS_TOPIC;


/**
 * Apache Kafka processor responsible for training neural networks.
 * It receives weights updates from the ServerProcessor and sends gradients back.
 */
public class WorkerTrainingProcessor
        extends AbstractProcessor<Long, WeightsMessage> {

    private Producer<Long, GradientMessage> gradientMessageProducer;
    KeyValueStore<Long, LabeledDataWithAge> data;
    Map<Long, LogisticRegressionTaskSpark> logisticRegressionTaskSpark;
    private String testDataFilePath;

    private int maxBufferSize;

    public WorkerTrainingProcessor(int maxBufferSize, String testDataFilePath) {
        this.maxBufferSize = maxBufferSize;
        this.testDataFilePath = testDataFilePath;
    }

    @Override
    @SuppressWarnings(value = "unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        this.gradientMessageProducer = ProducerBuilder.build(
                "client-gradientMessageProducer-" + UUID.randomUUID().toString());

        this.data = (KeyValueStore<Long, LabeledDataWithAge>)
                context.getStateStore(WorkerApp.INPUT_DATA_BUFFER);

        this.logisticRegressionTaskSpark = new HashMap<>();

        for (long partitionKey = 0L; partitionKey < WorkerApp.numWorkers; partitionKey++) {
            this.logisticRegressionTaskSpark.put(partitionKey, new LogisticRegressionTaskSpark(this.testDataFilePath));
        }
    }

    /**
     * Handle Message from {@link ServerProcessor} containing weight updates
     *
     * @param partitionKey Apache Kafka partition key
     * @param message      Contains weight updates
     */
    @Override
    public void process(Long partitionKey, WeightsMessage message) {

//         System.out.println("WorkerTrainingProcessor - Received weightsMessage on partition " + partitionKey);

        // If the received message is the first of its kind on a partition
        // the LogisticRegressionTaskSpark has not been initialized yet
        if (!this.logisticRegressionTaskSpark.get(partitionKey).isInitialized()) {
            this.logisticRegressionTaskSpark.get(partitionKey).initialize(false);
        }

        // Set ML model's weights according to the by the ServerProcessor send parameters
        this.logisticRegressionTaskSpark.get(partitionKey).setWeights(message.getValues());
        ArrayList<LabeledDataWithAge> dataOnPartition = this.getDataOnPartition(partitionKey);

        // Calculate gradients based on the local data
        SerializableHashMap gradients = this.logisticRegressionTaskSpark.get(partitionKey)
                .calculateGradients(dataOnPartition);

        // Log model's performance
        Metrics metrics = this.logisticRegressionTaskSpark.get(partitionKey).getMetrics();
        final long numTuplesReceivedSoFar = dataOnPartition.stream()
                .map(LabeledDataWithAge::getInsertionID)
                .max(java.lang.Long::compareTo)
                .orElse(0L);
        System.out.println(String.format(
                "%d;%d;%d;%s;%s;%s;%s", new Date().getTime(),
                Math.toIntExact(partitionKey), message.getVectorClock(),
                this.logisticRegressionTaskSpark.get(partitionKey).getLoss(),
                metrics.getF1(),
                metrics.getAccuracy(),
                numTuplesReceivedSoFar
        ));

        // Wrap gradients in GradientMessage and send gradients to the ServerProcessor
        GradientMessage gradientsMsg = new GradientMessage(message.getVectorClock(),
                this.getKeyRange(partitionKey), gradients, partitionKey);
        this.gradientMessageProducer.send(new ProducerRecord<>(GRADIENTS_TOPIC, 0L, gradientsMsg));
    }

    /**
     * Get overall keyRange from the stored weights ({@link LogisticRegressionTaskSpark#getWeights()})
     *
     * @return KeyRange
     */
    private KeyRange getKeyRange(Long partitionKey) {
        Integer smallestKey = Collections.min(this.logisticRegressionTaskSpark.get(partitionKey).getWeights().keySet());
        Integer largestKey = Collections.max(this.logisticRegressionTaskSpark.get(partitionKey).getWeights().keySet());
        return new KeyRange(smallestKey, largestKey);
    }

    /**
     * Extract data tuples from state store
     *
     * @param partitionKey current partition key
     * @return ArrayList containing all relevant data tuples
     */
    private ArrayList<LabeledDataWithAge> getDataOnPartition(Long partitionKey) {
        long startOfDataKeySpace = partitionKey * this.maxBufferSize;
        long endOfDataKeySpace = startOfDataKeySpace + this.maxBufferSize;
        KeyValueIterator<Long, LabeledDataWithAge> iterator =
                this.data.range(startOfDataKeySpace, endOfDataKeySpace);

        ArrayList<LabeledDataWithAge> dataOnPartition = new ArrayList<>();
        while (iterator.hasNext()) {
            dataOnPartition.add(iterator.next().value);
        }
        iterator.close();

        // NOTE: The condition below should never be met
        // because the producer is started before the app is initialized
        if (dataOnPartition.isEmpty()) {
            throw new IllegalStateException(String.format("There is no data for partition %d", partitionKey));
        }

        return dataOnPartition;
    }


//    /**
//     * Handles the waiting for new data to arrive. Once enough data was received
//     * the training loop with the {@link ServerProcessor} will be started.
//     *
//     * @param timestamp
//     */
//    private void waitForTrainingData(long timestamp) {
//        long partitionKey = this.context.taskId().partition;
//
//        KeyValueIterator<Long, MyArrayList<LabeledDataWithAge>> iterator =
//                this.data.range(partitionKey, partitionKey);
//
//        // Wait for next iteration if there is still no data on the current partition
//        if (!iterator.hasNext()) {
//            System.out.println(String.format("waitForTrainingData on partition: %d - !hasNext()", partitionKey));
//            return;
//        }
//
//        if (!this.logisticRegressionTaskSpark.isInitialized()) {
//            System.out.println(String.format("waitForTrainingData on partition: %d - !isInitialized()", partitionKey));
//            return;
//        }
//
//        MyArrayList<LabeledDataWithAge> dataOnCurrentPartition = iterator.next().value;
//
//        if (dataOnCurrentPartition.size() >= this.batchSize) {
//            // Cancel waiting for training data once we received enough data
//            this.trainingDataWaiter.cancel();
//            // Commit the current processing progress
//            this.context.commit();
//
//            /* Send an empty gradient message indicating
//            that we want to have an update of the weights */
////            GradientMessage emptyGradient = new GradientMessage(START_VC,
////                    new KeyRange(0, 0), new HashMap<>(), partitionKey);
////            this.sendGradients(emptyGradient);
//
//            System.out.println(String.format("Found enough data to start training. " +
//                            "Asked ServerProcessor for new weights at %d on partition %d",
//                    timestamp, partitionKey));
//        }
//    }
//
//    /**
//     * Handles the deletion of data entries
//     * that are older than {@link WorkerTrainingProcessor#maxAgeData}
//     *
//     * @param timestamp
//     */
//    private void deleteAgedData(long timestamp) {
//        long partitionKey = this.context.taskId().partition;
//
//        KeyValueIterator<Long, MyArrayList<LabeledDataWithAge>> iterator =
//                this.data.range(partitionKey, partitionKey);
//
//        // Wait for next iteration if there is no data on the current partition
//        if (!iterator.hasNext()) {
//            return;
//        }
//
//        MyArrayList<LabeledDataWithAge> dataOnCurrentPartition = iterator.next().value;
//
//        int deletionCounter = 0;
//        for (LabeledDataWithAge dataEntry : dataOnCurrentPartition.getData()) {
//            if (dataEntry.getAge() > this.maxAgeData) {
//                dataOnCurrentPartition.remove(dataEntry);
//                deletionCounter++;
//            }
//        }
//
//        System.out.println(String.format("Deleted %d data entries due to aging. ", deletionCounter));
//
//        // Update the data store with the filtered data
//        this.data.put(partitionKey, dataOnCurrentPartition);
//        // Commit the current processing progress
//        this.context.commit();
//    }
}

