package de.hpi.datastreams.apps;

import de.hpi.datastreams.messages.GradientMessage;
import de.hpi.datastreams.messages.KeyRange;
import de.hpi.datastreams.messages.WeightsMessage;
import de.hpi.datastreams.producer.CsvProducer;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.HashMap;

import static de.hpi.datastreams.apps.App.START_VC;


class AppRunner {
    public static void main(String[] args) {
        App server = new App(20);

        String pathToCSV = new File("./mockData//sample_input_data.csv").getAbsolutePath();
        CsvProducer producer = new CsvProducer(pathToCSV);

        try {
            server.call();

            Thread.sleep(1000);
            GradientMessage gradientMessage = new GradientMessage(START_VC, new KeyRange(0, 1), new HashMap<>());
            Producer<Long, GradientMessage> gradientMessageProducer = ProducerBuilder.build("client-initial-gradientMessageProducer");
            gradientMessageProducer.send(new ProducerRecord<>(App.GRADIENTS_TOPIC, 0L, gradientMessage));

            WeightsMessage weightsMessage = new WeightsMessage(START_VC, new KeyRange(0, 1), new HashMap<>());
            Producer<Long, WeightsMessage> weightsMessageProducer = ProducerBuilder.build("client-initial-weightsMessageProducer");
            weightsMessageProducer.send(new ProducerRecord<>(App.WEIGHTS_TOPIC, 0L, weightsMessage));

            producer.runProducer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
