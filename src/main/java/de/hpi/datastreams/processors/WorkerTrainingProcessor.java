package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.App;
import de.hpi.datastreams.messages.GradientMessage;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.MyArrayList;
import de.hpi.datastreams.messages.WeightsMessage;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;
import java.util.stream.IntStream;

import static de.hpi.datastreams.apps.App.GRADIENTS_TOPIC;
import static de.hpi.datastreams.apps.App.START_VC;


/**
 * Apache Kafka processor responsible for training neural networks.
 * It receives weights updates from the ServerProcessor and sends gradients back.
 */
public class WorkerTrainingProcessor extends AbstractProcessor<Long, WeightsMessage> {

    private HashMap<Integer, Float> weights;
    private Producer<Long, GradientMessage> gradientMessageProducer;
    KeyValueStore<Long, MyArrayList<Map<Integer, Float>>> data;

    @Override
    @SuppressWarnings(value = "unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        this.weights = new HashMap<>();
        this.gradientMessageProducer = ProducerBuilder.build(
                "client-gradientMessageProducer-" + UUID.randomUUID().toString());

        this.data = (KeyValueStore<Long, MyArrayList<Map<Integer, Float>>>)
                context.getStateStore(App.INPUT_DATA_BUFFER);
    }

    /**
     * Handle Message from ServerProcessor containing weight updates
     *
     * @param partitionKey Apache Kafka partition key
     * @param message      Contains weight updates
     */
    @Override
    public void process(Long partitionKey, WeightsMessage message) {

        // Do not handle initial message
        if (message.getVectorClock().equals(START_VC)) {
            return;
        }

        // TODO: remove
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Extract weights from message and store in local HashMap
        IntStream.range(message.getKeyRangeStart(), message.getKeyRangeEnd()).forEach(key -> {
            Optional<Float> weight = message.getValue(key);
            weight.ifPresent(aFloat -> weights.put(key, aFloat));
        });

        KeyValueIterator<Long, MyArrayList<Map<Integer, Float>>> iterator =
                this.data.range(partitionKey, partitionKey);

        // No need to run ML model if no data is available
        if (!iterator.hasNext()) {
            System.out.println(String.format(
                    "TrainingProcessor (partition %d) - received WeightsMessage - DataCount: %d",
                    Math.toIntExact(partitionKey), 0));

            // TODO: think of aborting here due to the lack of training data,
            //  but keep in mind the possible influence on the loop
            //  between ServerProcessor and WorkerTrainingProcessor
            // return;
        } else {
            int numEntries = iterator.next().value.size();
            System.out.println(String.format(
                    "TrainingProcessor (partition %d) - received WeightsMessage - DataCount: %d",
                    Math.toIntExact(partitionKey), numEntries));
        }

        // TODO: start training iteration
        GradientMessage gradients = new GradientMessage(message.getVectorClock(), this.getKeyRange(), new HashMap<>(), partitionKey);

        // Write calculated gradients to GRADIENTS_TOPIC stream
        System.out.println(String.format(
                "TrainingProcessor (partition %d) - send GradientMessage %s",
                Math.toIntExact(partitionKey), gradients.toString()));
        this.gradientMessageProducer.send(new ProducerRecord<>(GRADIENTS_TOPIC, 0L, gradients));
    }

    /**
     * Get overall keyRange from the stored weights
     *
     * @return KeyRange
     */
    private KeyRange getKeyRange() {
        Integer smallestKey = Collections.min(this.weights.keySet());
        Integer largestKey = Collections.max(this.weights.keySet());
        return new KeyRange(smallestKey, largestKey);
    }
}
