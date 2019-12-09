package de.hpi.datastreams.consumer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MyTest {

    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
//    private ConsumerRecordFactory<String, Message> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new JSONSerde<>());

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
//        Consumer.createStream(builder);
//        testDriver = new TopologyTestDriver(builder.build(), Main.getStreamsConfig()); // TODO
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
//        Message msg = new Message(0, new KeyRange(0, 0), new HashMap<>());
//        ConsumerRecord<byte[], byte[]> r = recordFactory.create(Consumer.INPUT_TOPIC, "", msg);
//        testDriver.pipeInput(r);
//
//        ProducerRecord<String, Message> outputRecord = testDriver.readOutput(
//                Consumer.OUTPUT_TOPIC,
//                stringDeserializer,
//                new JSONSerde<>());
//
//        OutputVerifier.compareKeyValue(outputRecord, "", msg);
//        assertNull(testDriver.readOutput(Consumer.OUTPUT_TOPIC, stringDeserializer, new JSONSerde<>()));
    }
}
