package de.hpi.datastreams.producer;

import de.hpi.datastreams.messages.LabeledData;
import de.hpi.datastreams.ml.LogisticRegressionTaskSpark;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.hpi.datastreams.apps.WorkerApp.INPUT_DATA_NUM_PARTITIONS;

public class CsvProducer {

    private Producer<Long, LabeledData> producer;
    private String csvPath;
    private String topicName;

    public CsvProducer(String csvPath, String topicName) {
        this.csvPath = csvPath;
        this.topicName = topicName;
        this.producer = ProducerBuilder.build("client-dataMessageProducer-" + UUID.randomUUID().toString());
    }

    public void runProducer(Boolean hasHeader) throws IOException {
        BufferedReader csvReader = new BufferedReader(new FileReader(this.csvPath));
        String row;
        int rowCount = 0;

        if (hasHeader) {
            row = csvReader.readLine();
        }

        // Read csv file
        while ((row = csvReader.readLine()) != null) {
            // TODO: remove
            String[] data = row.split(",");
            int length = data.length;
            assert length == LogisticRegressionTaskSpark.numFeatures + 1;

            // Convert csv row into messages
            Map<Integer, Float> inputData = IntStream
                    .range(0, length - 1)
                    .filter(i -> Float.parseFloat(data[i]) != 0f)
                    .mapToObj(i -> new AbstractMap.SimpleEntry<>(i, Float.valueOf(data[i])))
                    .filter(entry -> entry.getValue() != 0f)
                    .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
            int label = Integer.parseInt(data[length - 1]);

            LabeledData dataMessage = new LabeledData(inputData, label);
            ProducerRecord<Long, LabeledData> record = new ProducerRecord<>(this.topicName, (long) (rowCount % INPUT_DATA_NUM_PARTITIONS), dataMessage);

            // Send message into queue
            try {
                RecordMetadata metadata = producer.send(record).get();
//                System.out.println("Record sent with key " + rowCount + " to partition " + metadata.partition()
//                        + " with offset " + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }

            rowCount++;

            if (rowCount >= 4 * 128) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("CSV Producer sent all records!");
        csvReader.close();
    }

    public Thread runProducerInBackground() throws IOException {
        return new Thread(() -> {
            try {
                this.runProducer(true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }
}

