package de.hpi.datastreams;

import de.hpi.datastreams.consumer.ConsumerCreator;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.Message;
import de.hpi.datastreams.producer.ProducerCreator;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.streams.StreamsConfig;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) {
        runProducer();
        runConsumer();
    }
    static void runConsumer() {
        Consumer<Long, Message> consumer = ConsumerCreator.createConsumer();
        //Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, Message> consumerRecords = consumer.poll(1000);
            //ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value().toString());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }

    static void runProducer() {
        Producer<Long, Message> producer = ProducerCreator.createProducer();
        //Producer<Long, String> producer = ProducerCreator.createProducer();
        for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            Message msg = new Message(index, new KeyRange(0, 0), new ArrayList<>());
            ProducerRecord<Long, Message> record = new ProducerRecord<Long, Message>(IKafkaConstants.TOPIC_NAME, msg);
            //ProducerRecord<Long, String> string_record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME, "test message");
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            }
            catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }
}
