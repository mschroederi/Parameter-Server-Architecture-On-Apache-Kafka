package de.hpi.datastreams.processors;

import de.hpi.datastreams.messages.GradientMessage;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.WeightsMessage;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.hpi.datastreams.apps.App.START_VC;
import static de.hpi.datastreams.apps.App.WEIGHTS_TOPIC;

/**
 * Apache Kafka processor responsible for synchronizing the TrainingProcessors.
 * It receives gradients updates from the TrainingProcessors and sends weights back.
 */
public class ServerProcessor extends AbstractProcessor<Long, GradientMessage> {

    private HashMap<Integer, Float> weights;
    private Float learningRate = 1e-2f;
    private Producer<Long, WeightsMessage> weightsMessageProducer;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.weights = new HashMap<>();
        this.weightsMessageProducer = ProducerBuilder.build("client-weightsMessageProducer-" + UUID.randomUUID().toString());
    }

    /**
     * Handle Message from TrainingProcessor containing calculated gradients
     *
     * @param partitionKey Apache Kafka partition key
     * @param message      Contains gradients
     */
    @Override
    public void process(Long partitionKey, GradientMessage message) {
        System.out.println(String.format(
                "ServerProcessor (partition %d) received: %s",
                Math.toIntExact(partitionKey), message.toString()));

        // Initialize ML model when receiving initial message
        if (message.getVectorClock().equals(START_VC)) {
            this.initializeModel();
        }

        // Extract gradients from message and update locally stored weights
        IntStream.range(message.getKeyRangeStart(), message.getKeyRangeEnd()).forEach(key -> {
            Optional<Float> gradient = message.getValue(key);
            gradient.ifPresent(partialGradient -> updateWeight(key, partialGradient));
        });

        // Start new training iteration on workers by sending update of weights
        // TODO: Currently only eventual consistency model supported
        KeyRange keyRange = this.getKeyRange();
        WeightsMessage weights = new WeightsMessage(message.getVectorClock() + 1, keyRange, this.getWeights(keyRange));

        this.weightsMessageProducer.send(new ProducerRecord<>(WEIGHTS_TOPIC, message.getPartitionKey(), weights));
    }

    private void initializeModel() {
        // TODO: initialize weights
        this.weights.put(0, 0f);
        this.weights.put(1, 0f);
    }

    /**
     * Get overall keyRange from the stored weights
     *
     * @return KeyRange
     */
    private KeyRange getKeyRange() {
        if (this.weights.keySet().isEmpty()) {
            return new KeyRange(0, 0);
        }

        Integer smallestKey = Collections.min(this.weights.keySet());
        Integer largestKey = Collections.max(this.weights.keySet());
        return new KeyRange(smallestKey, largestKey);
    }

    /**
     * Get weights contained in key range
     *
     * @param keyRange Defines the weights to select
     * @return Map containing the requested weights
     */
    private Map<Integer, Float> getWeights(KeyRange keyRange) {

        return this.weights.entrySet().stream()
                .filter(entrySet -> keyRange.contains(entrySet.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (prev, next) -> next, HashMap::new));
    }

    /**
     * Update the weight of a specific key given a gradient with respect to the learning rate
     *
     * @param key      Identified weight to update
     * @param gradient Calculated gradient for the given key
     */
    private void updateWeight(Integer key, Float gradient) {
        Float oldWeight = this.weights.get(key);
        Float newWeight = oldWeight - this.learningRate * gradient;
        this.weights.put(key, newWeight);
    }
}
