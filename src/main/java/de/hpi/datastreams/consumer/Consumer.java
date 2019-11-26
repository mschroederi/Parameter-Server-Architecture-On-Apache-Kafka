package de.hpi.datastreams.consumer;

import de.hpi.datastreams.messages.Message;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


public class Consumer {

    public final static String INPUT_TOPIC = "input";
    public final static String OUTPUT_TOPIC = "output";

    static void createStream(final StreamsBuilder builder) {
        final KStream<String, Message> source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), new JSONSerde<>()));
        source.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), new JSONSerde<>()));
    }
}
