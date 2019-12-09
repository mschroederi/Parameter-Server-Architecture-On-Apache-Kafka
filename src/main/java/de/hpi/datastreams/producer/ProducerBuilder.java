package de.hpi.datastreams.producer;

import de.hpi.datastreams.apps.BaseKafkaApp;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

import java.util.Properties;

public class ProducerBuilder {
    public static <T> Producer<Long, T> build(String clientID) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BaseKafkaApp.brokers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class.getName());
        return new KafkaProducer<>(props);
    }
}
