package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.WorkerApp;
import de.hpi.datastreams.messages.GradientMessage;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.SerializableHashMap;
import de.hpi.datastreams.messages.WeightsMessage;
import de.hpi.datastreams.ml.LogisticRegressionTaskSpark;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.commons.lang.math.LongRange;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static de.hpi.datastreams.apps.WorkerApp.*;

/**
 * Apache Kafka processor responsible for synchronizing the TrainingProcessors.
 * It receives gradients updates from the TrainingProcessors and sends weights back.
 */
public class ServerProcessor extends AbstractProcessor<Long, GradientMessage> {

    private ProcessorContext context;

    private SerializableHashMap weights;
    private final Float learningRate = 1f / numWorkers;
    private Producer<Long, WeightsMessage> weightsMessageProducer;

    LogisticRegressionTaskSpark lgTask = new LogisticRegressionTaskSpark();
    private MessageTracker messageTracker = new MessageTracker(WorkerApp.numWorkers);

    private Cancellable trainingLoopStarter;

    public static final int MAX_DELAY_INFINITY = -1;
    private final Integer maxVectorClockDelay = 1;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.context = context;

        this.weights = new SerializableHashMap();
        this.weightsMessageProducer = ProducerBuilder.build("client-weightsMessageProducer-" + UUID.randomUUID().toString());

        try {
            this.trainingLoopStarter = this.context.schedule(1000,
                    PunctuationType.WALL_CLOCK_TIME, this::startTrainingLoop);
        } catch (IllegalArgumentException ignored) {
            System.out.println(ignored.toString());
        }
    }

    /**
     * Starts the training loop between the {@link ServerProcessor}
     * and {@link WorkerTrainingProcessor}s by initializing the ML model's weights
     * and sending them to all workers
     *
     * @param timestamp
     */
    private void startTrainingLoop(long timestamp) {
        // Initialize the ML model's weights
        this.setInitialWeights();

        WeightsMessage weightsMessage = new WeightsMessage(0, this.getKeyRange(), this.getWeights());
        LongStream.range(0, WEIGHTS_TOPIC_NUM_PARTITIONS).forEach(partitionKey -> {
            this.weightsMessageProducer.send(new ProducerRecord<>(WEIGHTS_TOPIC, partitionKey, weightsMessage));
        });

        this.context.commit();
        // Just execute the training loop starter once
        this.trainingLoopStarter.cancel();
    }

    /**
     * Decides based on the chosen consistency model which workers to answer.
     *
     * @param receivedVC vector clock of the message received in {@link ServerProcessor#process(Long, GradientMessage)}
     * @return Set of partition keys
     */
    private HashSet<Tuple2<Long, Integer>> workersToRespondTo(Integer receivedVC, Long receivedPartitionKey) {
        HashSet<Tuple2<Long, Integer>> workersToAnswers = new HashSet<>();

        /*
         * Directly answer with message containing weights
         * if the user chose the eventual consistency model.
         */
        if (this.maxVectorClockDelay == MAX_DELAY_INFINITY) {
            workersToAnswers.add(new Tuple2<>(receivedPartitionKey, receivedVC + 1));
            this.messageTracker.sentMessage(receivedPartitionKey, receivedVC + 1);
        }
        /*
         * Send new batch of messages containing weights
         * if the user chose the sequential consistency model
         *  and we already received all messages containing the recently received vector clock.
         */
        else if (this.maxVectorClockDelay == 0
                && this.messageTracker.hasReceivedAllMessages(receivedVC)) {
            List<Tuple2<Long, Integer>> allPartitionKeys =
                    Arrays.stream(new LongRange(0L, WorkerApp.numWorkers - 1).toArray())
                            .boxed()
                            .map(partitionKey -> new Tuple2<>(partitionKey, receivedVC + 1))
                            .collect(Collectors.toList());
            workersToAnswers.addAll(allPartitionKeys);
            this.messageTracker.sentAllMessages(receivedVC + 1);
        }
        /*
         * If the user chose the bounded delay consistency model,
         * send back weights to the those workers whose current vector clock is
         * less than `maxVectorClockDelay` ahead of minimum outstanding vector clock
         */
        else if (this.maxVectorClockDelay > 0) {

            ArrayList<Tuple2<Long, Integer>> sendableMessages =
                    this.messageTracker.getAllSendableMessages(receivedVC, this.maxVectorClockDelay);
            workersToAnswers.addAll(sendableMessages);
        }

        return workersToAnswers;
    }

    /**
     * Handle Message from TrainingProcessor containing calculated gradients
     *
     * @param partitionKey Apache Kafka partition key
     * @param message      Contains gradients
     */
    @Override
    public void process(Long partitionKey, GradientMessage message) {
        // Register message in vector clock tracker
        this.messageTracker.receivedMessage(message.getPartitionKey(), message.getVectorClock());

        // Extract gradients from message and update locally stored weights
        IntStream.range(message.getKeyRangeStart(), message.getKeyRangeEnd()).forEach(key -> {
            Optional<Float> gradient = message.getValue(key);
            gradient.ifPresent(partialGradient -> updateWeight(key, partialGradient));
        });

        // Log weights every 10 vector clocks
        if (message.getVectorClock() % 1 == 0 && message.getPartitionKey() == 0) {
            this.lgTask.setWeights(this.weights);
            this.lgTask.calculateTestMetrics();

            System.out.println(String.format(
                    "%d;%d;%d;%s;%s;%s", new Date().getTime(),
                    Math.toIntExact(-1), message.getVectorClock(),
                    -1,
                    this.lgTask.getMetrics().getF1(),
                    this.lgTask.getMetrics().getAccuracy()
            ));
        }

        /*
         * Start new training iteration on workers by sending update of weights.
         * The Set of workers that will receive a weight update is based on
         * the chosen consistency model and the maximum allowed vector clock delay
         */
        this.workersToRespondTo(message.getVectorClock(), message.getPartitionKey())
                .forEach((tuple) -> {
                    Long workerPartitionKey = tuple._1;
                    Integer vectorClock = tuple._2;
                    WeightsMessage weights = new WeightsMessage(vectorClock,
                            this.getKeyRange(), this.getWeights());
                    this.weightsMessageProducer.send(
                            new ProducerRecord<>(WEIGHTS_TOPIC, workerPartitionKey, weights));

                    this.messageTracker.sentMessage(tuple._1, tuple._2);
                });
    }

    /**
     * Generates and stores the ML model's initial weights.
     */
    private void setInitialWeights() {
        this.lgTask.initialize(true);
        this.weights = this.lgTask.getWeights();
    }

    /**
     * Get overall keyRange from the stored weights
     *
     * @return KeyRange
     */
    private KeyRange getKeyRange() {
        Integer smallestKey = 0;
        Integer largestKey = 0;

        if (!this.weights.isEmpty()) {
            smallestKey = Collections.min(this.weights.keySet());
            largestKey = Collections.max(this.weights.keySet());
        }

        return new KeyRange(smallestKey, largestKey + 1);
    }

    /**
     * Get all weights.
     *
     * @return Map containing all weights
     */
    private SerializableHashMap getWeights() {
        return this.weights;
    }

    /**
     * Update the weight of a specific key given a gradient with respect to the learning rate
     *
     * @param key      Identified weight to update
     * @param gradient Calculated gradient for the given key
     */
    private void updateWeight(Integer key, Float gradient) {
        Float oldWeight = this.weights.get(key);
        this.weights.put(key, oldWeight + this.learningRate * gradient);
    }
}

