package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;


class PredictionAppRunner {
    public static void main(String[] args) throws FileNotFoundException {

        PrintStream fileOut = new PrintStream("./log_prediction.csv");
        System.setOut(fileOut);

        // Turn off logging
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        PredictionApp predictor = new PredictionApp(""); // TODO

        try {
            // Predict output to the data in the PREDICTION_DATA_TOPIC, using the weights from the server
            predictor.call();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

