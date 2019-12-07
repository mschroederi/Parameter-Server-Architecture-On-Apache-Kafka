package de.hpi.datastreams.producer;

import java.util.Properties;

import de.hpi.datastreams.messages.Message;
import de.hpi.datastreams.serialization.JSONSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

import de.hpi.datastreams.IKafkaConstants;

public class ProducerCreator {
    public static Producer<Long, Message> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class.getName());
        return new KafkaProducer<>(props);
    }
}
