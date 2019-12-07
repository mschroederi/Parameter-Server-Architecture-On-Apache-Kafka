package de.hpi.datastreams.producer;

import de.hpi.datastreams.apps.SamplingApp;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.KeyValue;
import de.hpi.datastreams.messages.Message;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CsvProducer {

    Producer<Long, Message> producer;
    String csvPath;
    String topicName;

    public CsvProducer(String csvPath){
        this.csvPath = csvPath;
        this.topicName = SamplingApp.INPUT_DATA;
        this.producer = ProducerCreator.createProducer();
    }

    void runProducer() throws IOException {
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
                    .mapToObj(i -> new KeyValue(i, Float.valueOf(data[i])))
                    .collect(Collectors.toMap(KeyValue::key, KeyValue::value));
                    //.collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));

            Message msg = new Message(rowCount, new KeyRange(0, length-1), mappedData);
            ProducerRecord<Long, Message> record = new ProducerRecord<Long, Message>(this.topicName, msg);

            // Send message into queue
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + rowCount + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            }
            catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }

            rowCount++;
        }
        csvReader.close();
    }
}

class CsvProducerRunner{
    public static void main(String[] args) {
        String pathToCSV = new File("./mockData//sample_input_data.csv").getAbsolutePath();
        CsvProducer producer = new CsvProducer(pathToCSV);
        try {
            producer.runProducer();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
