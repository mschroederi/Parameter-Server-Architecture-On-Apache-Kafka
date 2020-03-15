package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.sources.In;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;


class ServerAppRunner {
    public static void main(String[] args) throws IOException, ParseException {

        // create options
        Options options = new Options();

        Option option_training_data = new Option("training", "training_data_link", true, "[REQUIRED] The link to an csv file that is used as training data.");
        Option option_test_data = new Option("test", "test_data_link", true, "[REQUIRED] The link to an csv file that is used as test data (to compute statistics)");
        Option option_consistency_model = new Option("c", "consistency_model", true, "An number that defines the consistency model (see README for details)");
        Option option_producer_time_per_event = new Option("p", "producer_time_per_event", true, "This is used to artificially increase/decrease the amount of events that created.");
        Option option_verbose = new Option("v", "verbose", false, "If enabled, prints the parameter that are used");
        Option option_help = new Option("h", "help", false, "Show list of possible parameter");
        Option option_broker = new Option("r", "remote", false, "If disabled, 'localhost' is used as the IP for the broker");

        // mark required options
        option_training_data.setRequired(true);
        option_test_data.setRequired(true);

        options.addOption(option_training_data)
                .addOption(option_test_data)
                .addOption(option_consistency_model)
                .addOption(option_producer_time_per_event)
                .addOption(option_verbose)
                .addOption(option_help)
                .addOption(option_broker);


        // parse the command line parameter
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse( options, args);

        if(cmd.hasOption("help")){
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ServerAppRunner", options);
            System.exit(0);
        }

        if(cmd.getArgs().length > 0){
            // There are args that could not be associated with a existing parameter
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ServerAppRunner", options);
            System.exit(2);
        }

        // compute the arguments
        String trainingDataFile = cmd.getOptionValue("training_data_link"); // TODO: use the link to download file
        String testDataFile = cmd.getOptionValue("test_data_link"); // TODO: use the link to download the file
        boolean usePrediction = false;  // TODO
        int consistencyModel = Integer.parseInt(cmd.getOptionValue("consistency_model", "0"));
        int producerTimePerEvent = Integer.parseInt(cmd.getOptionValue("producer_time_per_event", "200"));
        boolean verbose = cmd.hasOption("verbose");
        boolean isRemote = cmd.hasOption("remote");
        String broker = isRemote ? "kafka-0:9092" : "localhost:29092";

        // print used parameter
        if(verbose){
            System.out.println();
            System.out.println("Used parameter:");
            System.out.println("    training_data_link: " + trainingDataFile);
            System.out.println("    test_data_link: " + testDataFile);
            System.out.println("    consistency_model: " + consistencyModel);
            System.out.println("    producer_time_per_event: " + producerTimePerEvent);
            System.out.println("    broker address: " + broker);
            System.out.println();
        }


        CsvProducer inputDataProducer;
        CsvProducer predictionDataProducer;

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");

        BaseKafkaApp.brokers = broker;
        ServerApp server = new ServerApp(consistencyModel, testDataFile);

        predictionDataProducer = new CsvProducer(testDataFile, WorkerApp.PREDICTION_DATA_TOPIC, producerTimePerEvent);
        inputDataProducer = new CsvProducer(trainingDataFile, WorkerApp.INPUT_DATA_TOPIC, producerTimePerEvent);

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

