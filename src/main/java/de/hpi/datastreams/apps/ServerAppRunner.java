package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;


class ServerAppRunner {
    public static void main(String[] args) throws FileNotFoundException {

        String trainingDataFile = "";
        String predictionDataFile = "";
        boolean usePrediction = false;

        CsvProducer inputDataProducer;
        CsvProducer predictionDataProducer;

        if(args.length == 0){
            System.out.println("Expected the path to the training data as parameter");
            System.exit(1);
        } else if (args.length > 1){
            predictionDataFile = new File(args[1]).getAbsolutePath();
            usePrediction = true;
        }
        trainingDataFile = new File(args[0]).getAbsolutePath();

        PrintStream fileOut = new PrintStream("./log_server.csv");
        System.setOut(fileOut);

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");

        // Turn off logging
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        ServerApp server = new ServerApp();

        predictionDataProducer = new CsvProducer(predictionDataFile, WorkerApp.PREDICTION_DATA_TOPIC);
        inputDataProducer = new CsvProducer(trainingDataFile, WorkerApp.INPUT_DATA_TOPIC);

        try {
            // Generate stream data in background
            Thread inputDataThread = inputDataProducer.runProducerInBackground();
            inputDataThread.start();

            Thread.sleep(10000);

            server.call();

            if(usePrediction) {
                Thread.sleep(10000); // wait 10 sec before starting predictions
                Thread predictionDataThread = predictionDataProducer.runProducerInBackground();
                predictionDataThread.start();
                predictionDataThread.join();
            }

            inputDataThread.join();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

