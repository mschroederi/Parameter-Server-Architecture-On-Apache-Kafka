package de.hpi.datastreams.consumer;

import de.hpi.datastreams.Main;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.Message;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertNull;

public class MyTest {

    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private ConsumerRecordFactory<String, Message> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JSONSerde<>());

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        Consumer.createStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), Main.getStreamsConfig());
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            System.out.println("Ignoring exception, test failing due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testSimpleMessage() {
        Message msg = new Message(0, new KeyRange(0, 0), new ArrayList<>());
        ConsumerRecord<byte[], byte[]> r = recordFactory.create(Consumer.INPUT_TOPIC, "", msg);
        testDriver.pipeInput(r);

        ProducerRecord<String, Message> outputRecord = testDriver.readOutput(
                Consumer.OUTPUT_TOPIC,
                stringDeserializer,
                new JSONSerde<>());

        OutputVerifier.compareKeyValue(outputRecord, "", msg);
        assertNull(testDriver.readOutput(Consumer.OUTPUT_TOPIC, stringDeserializer, new JSONSerde<>()));
    }
}
