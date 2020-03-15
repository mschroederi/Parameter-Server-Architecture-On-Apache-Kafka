package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;

import java.io.IOException;

import static de.hpi.datastreams.apps.BaseKafkaApp.TRAINING_DATA_FILE_PATH;


class ServerAppRunner {

    public static void main(String[] args) throws IOException {

        String predictionDataFile = "";
        boolean usePrediction = false;

        ServerApp.downloadDatasetsIfNecessary();

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

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");
        ServerApp server = new ServerApp();

        CsvProducer predictionDataProducer = new CsvProducer(predictionDataFile, WorkerApp.PREDICTION_DATA_TOPIC);
        CsvProducer inputDataProducer = new CsvProducer(TRAINING_DATA_FILE_PATH, WorkerApp.INPUT_DATA_TOPIC);

        try {
            // Generate stream data in background
            Thread inputDataThread = inputDataProducer.runProducerInBackground();
            inputDataThread.start();

            Thread.sleep(10000);

            server.call();

            if (usePrediction) {
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

