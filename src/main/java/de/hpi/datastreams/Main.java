package de.hpi.datastreams;

import de.hpi.datastreams.consumer.ConsumerCreator;
import de.hpi.datastreams.messages.Message;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Main {

    public static void main(String[] args) {
        //runConsumer();
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
}
