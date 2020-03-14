package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;


class ServerAppRunner {
    public static void main(String[] args) throws IOException {

        String trainingDataFile = "mockData/small-sample/small_sample_reviews_embedded_training.csv";
        //String trainingDataFile = "./local_trainings_data.csv";
        String predictionDataFile = "";
        boolean usePrediction = false;

        CsvProducer inputDataProducer;
        CsvProducer predictionDataProducer;

        /*
        if(args.length == 0){
            System.out.println("Expected the path to the training data as parameter");
            System.exit(1);
        } else if (args.length > 1){
            predictionDataFile = new File(args[1]).getAbsolutePath();
            usePrediction = true;
        }
        trainingDataFile = new File(args[0]).getAbsolutePath();

         */

        // Download tracings- and test-data
        /*
        URL url = new URL("https://s3.eu-central-1.amazonaws.com/de.hpi.datastreams.parameter-server/small-test.csv");
        File trainingsData = new File(trainingDataFile);
        FileUtils.copyURLToFile(url, trainingsData);
         */

        // PrintStream fileOut = new PrintStream("./log_server.csv");
        // System.setOut(fileOut);

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");

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

