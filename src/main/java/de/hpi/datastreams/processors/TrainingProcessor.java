package de.hpi.datastreams.processors;

import de.hpi.datastreams.Config;
import de.hpi.datastreams.IKafkaConstants;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.Message;
import de.hpi.datastreams.producer.ProducerCreator;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import java.util.stream.IntStream;

public class TrainingProcessor extends AbstractProcessor<Long, Message> {

    static String WEIGHTS_STORE = "WEIGHTS_STORE";
    private HashMap<Integer, Float> weights;

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.weights = new HashMap<>();
    }

    @Override
    public void process(Long partitionKey, Message gradientMessage) {
        IntStream.range(gradientMessage.getKeyRange().getStart(), gradientMessage.getKeyRange().getEnd()).forEach(key -> {
            Optional<Float> weight = gradientMessage.getValue(key);
            weight.ifPresent(aFloat -> weights.put(key, aFloat));
        });
    }

    static void createGradientStream(final StreamsBuilder builder) {
        final KStream<String, Message> source = builder.stream(Config.WEIGHTS_TOPIC, Consumed.with(Serdes.String(), new JSONSerde<>()));

        // TODO: ML gradient calculation
        // 1. store local weights
        // 2. set weights in ML model
        // 3. calculate gradients


        // 4. extract gradients
        Message gradients = new Message(0, new KeyRange(0, 1), new HashMap<>());

        // 5. write gradients to GRADIENT_TOPIC
        Producer<Long, Message> gradientProducer = ProducerCreator.createProducer();
        gradientProducer.send(new ProducerRecord<>(IKafkaConstants.TOPIC_NAME, 0L, gradients));

//        builder.stream(Config.GRADIENT_TOPIC).

//        source.to(Config.GRADIENT_TOPIC, Produced.with(Serdes.String(), new JSONSerde<>()));
    }
}
