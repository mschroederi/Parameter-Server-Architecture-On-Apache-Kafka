package de.hpi.datastreams.producer;

import de.hpi.datastreams.Main;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Vector;

public class ProducerTest {

    private TopologyTestDriver testDriver;
    private StringDeserializer stringDeserializer = new StringDeserializer();
    private ByteArrayDeserializer byteArrayDeserializer = new ByteArrayDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
//        WordCountDemo.createWordCountStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), Main.getStreamsConfig());
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testOneWord() {
        Integer[] d = {1, 2, 3, 4, 5};
        Vector<Integer> data = new Vector<>(Arrays.asList(d));

        new Producer().produce(data);
        ProducerRecord<String, String> outputRecord = testDriver.readOutput(Producer.topicName, stringDeserializer, stringDeserializer);

        System.out.println(outputRecord.toString());

//        testDriver.pipeInput(recordFactory.create(INPUT_TOPIC, null, "Hello"));
//
//        ProducerRecord<String, Long> outputRecord = testDriver.readOutput(
//                OUTPUT_TOPIC,
//                stringDeserializer,
//                longDeserializer);
//
//        OutputVerifier.compareKeyValue(outputRecord, "hello", 1L);
//        assertNull(testDriver.readOutput(OUTPUT_TOPIC, stringDeserializer, longDeserializer));
    }
}
