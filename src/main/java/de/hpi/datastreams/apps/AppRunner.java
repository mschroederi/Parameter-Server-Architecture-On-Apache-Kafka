package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;


class AppRunner {
    public static void main(String[] args) throws FileNotFoundException {

        PrintStream fileOut = new PrintStream("./log.csv");
        System.setOut(fileOut);

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");

        // Turn off logging
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        App server = new App(128);

//        String pathToCSV = new File("./data/spam_embedded.csv").getAbsolutePath();
        String pathToCSV = new File("./data/reviews_embedded_train.csv").getAbsolutePath();
        CsvProducer inputDataProducer = new CsvProducer(pathToCSV, App.INPUT_DATA_TOPIC);
        CsvProducer predictionDataProducer = new CsvProducer(pathToCSV, App.PREDICTION_DATA_TOPIC); // in a real scenario this would not be the same csv file as the inputData

        try {
            // Generate stream data in background
            Thread inputDataThread = inputDataProducer.runProducerInBackground();
            inputDataThread.start();

            Thread.sleep(1000);
            server.call();



            Thread.sleep(10000); // wait 10 sec before starting predictions
//            Thread predictionDataThread = predictionDataProducer.runProducerInBackground();
//            predictionDataThread.start();

            inputDataThread.join();
//            predictionDataThread.join();

            // TODO: investigate the option of a "Scheduler" to avoid starting everything right from the beginning
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

