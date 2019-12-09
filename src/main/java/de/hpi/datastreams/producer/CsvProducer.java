package de.hpi.datastreams.producer;

import de.hpi.datastreams.apps.App;
import de.hpi.datastreams.messages.DataMessage;
import de.hpi.datastreams.messages.KeyRange;
import javafx.util.Pair;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvProducer {

    private Producer<Long, DataMessage> producer;
    private String csvPath;
    private String topicName;

    public CsvProducer(String csvPath) {
        this.csvPath = csvPath;
        this.topicName = App.INPUT_DATA_TOPIC;
        this.producer = ProducerBuilder.build("client-dataMessageProducer-" + UUID.randomUUID().toString());
    }

    public void runProducer() throws IOException {
        BufferedReader csvReader = new BufferedReader(new FileReader(this.csvPath));
        String row;
        int rowCount = 0;

        // Read csv file
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(",");
            int length = data.length;

            // Convert csv row into message
            Map<Integer, Float> mappedData = IntStream
                    .range(0, length)
                    .mapToObj(i -> new Pair<>(i, Float.valueOf(data[i])))
                    .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

            DataMessage dataMessage = new DataMessage(rowCount, new KeyRange(0, length - 1), mappedData);
            ProducerRecord<Long, DataMessage> record = new ProducerRecord<>(this.topicName, 0L, dataMessage);

            // Send message into queue
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + rowCount + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }

            rowCount++;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        csvReader.close();
    }
}

