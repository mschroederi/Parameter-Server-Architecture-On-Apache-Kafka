package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.App;
import de.hpi.datastreams.messages.DataMessage;
import de.hpi.datastreams.messages.GradientMessage;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.WeightsMessage;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
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
    KeyValueStore<Long, DataMessage> data;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.weights = new HashMap<>();
        this.gradientMessageProducer = ProducerBuilder.build("client-gradientMessageProducer-" + UUID.randomUUID().toString());

        this.data = (KeyValueStore<Long, DataMessage>) context.getStateStore(App.INPUT_DATA_BUFFER);
    }

    /**
     * Handle Message from ServerProcessor containing weight updates
     *
     * @param partitionKey Apache Kafka partition key
     * @param message      Contains weight updates
     */
    @Override
    public void process(Long partitionKey, WeightsMessage message) {
        System.out.println("TrainingProcessor - received: " + message.toString() + " - data count: " + this.data.approximateNumEntries());

        if (message.getVectorClock().equals(START_VC)) {
            return;
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        IntStream.range(message.getKeyRangeStart(), message.getKeyRangeEnd()).forEach(key -> {
            Optional<Float> weight = message.getValue(key);
            weight.ifPresent(aFloat -> weights.put(key, aFloat));
        });

        // TODO: start training iteration - sequential consistency model
        GradientMessage gradients = new GradientMessage(message.getVectorClock(), new KeyRange(0, 1), new HashMap<>());

        this.gradientMessageProducer.send(new ProducerRecord<>(GRADIENTS_TOPIC, 0L, gradients));
    }
}
