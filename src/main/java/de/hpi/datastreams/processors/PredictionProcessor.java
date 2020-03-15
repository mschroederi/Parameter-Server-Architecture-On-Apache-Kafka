package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.WorkerApp;
import de.hpi.datastreams.messages.*;
import de.hpi.datastreams.ml.LogisticRegressionTaskSpark;
import de.hpi.datastreams.producer.ProducerBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.UUID;

import static de.hpi.datastreams.apps.BaseKafkaApp.PREDICTION_OUTPUT_TOPIC;

public class PredictionProcessor extends AbstractProcessor<Long, LabeledData> {

    private KeyValueStore<Long, SerializableHashMap> weights;
    private Producer<Long, Float> predictionMessageProducer;
    LogisticRegressionTaskSpark logisticRegressionTaskSpark;
    private String testDataFilePath;

    public PredictionProcessor(String testDataFilePath){
        this.testDataFilePath = testDataFilePath;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.weights = (KeyValueStore<Long, SerializableHashMap>) context.getStateStore(WorkerApp.WEIGHTS_STORE);
        this.predictionMessageProducer = ProducerBuilder.buildFloatValue("predictionProducer-" + UUID.randomUUID().toString());
        logisticRegressionTaskSpark = new LogisticRegressionTaskSpark(this.testDataFilePath);
    }

    @Override
    public void process(Long partitionKey, LabeledData value) {
        System.out.println("Prediction process");

        // If the received message is the first one
        // the LogisticRegressionTaskSpark has not been initialized yet
        if (!this.logisticRegressionTaskSpark.isInitialized()) {
            this.logisticRegressionTaskSpark.initialize(false);
        }

        if (this.weights.get(0L) == null
                || this.weights.get(0L).size() == 0
                || value.getInputData().size() == 0
        ) {
            return;
        }

        // Set ML model's weights according to the weights from the Server
        SerializableHashMap w = this.weights.get(0L);
        this.logisticRegressionTaskSpark.setWeights(w);

        float prediction = this.logisticRegressionTaskSpark.predict(value);
        System.out.println("PredictionProcessor - Calculating prediction: " + prediction);

        //Send the prediction to the consumer
        this.predictionMessageProducer.send(new ProducerRecord<>(PREDICTION_OUTPUT_TOPIC, prediction));
    }
}
