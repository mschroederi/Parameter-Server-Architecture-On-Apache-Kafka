package de.hpi.datastreams.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.Vector;

public class Producer {

    public final static String topicName = "streams-input";

    public void produce(Vector<Integer> data) {

        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> rec = new ProducerRecord<>(topicName, data.toString());
            producer.send(rec);
        }
        producer.close();
    }

    public static void main(String[] args) {
        Integer[] d = {1, 2, 3, 4, 5};
        Vector<Integer> data = new Vector<>(Arrays.asList(d));

        new Producer().produce(data);
    }
}
