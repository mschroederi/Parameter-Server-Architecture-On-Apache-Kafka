package de.hpi.datastreams.apps;

import org.apache.commons.cli.*;

import java.io.IOException;
import java.io.PrintStream;

import static de.hpi.datastreams.apps.BaseKafkaApp.TEST_DATA_DEFAULT_URL;
import static de.hpi.datastreams.apps.BaseKafkaApp.TEST_DATA_FILE_PATH;


class WorkerAppRunner {

    final static String CMD_TEST_DATA_URL = "test_data_url";

    public static void main(String[] args) throws InterruptedException, ParseException, IOException {
        // create options
        Options options = new Options();

        Option option_test_data = new Option("test", CMD_TEST_DATA_URL, true, "The link to an csv file that is used as test data (to compute statistics)");
        Option option_min_buffer = new Option("min", "min_buffer_size", true, "The minimum buffer size that stores the incoming events");
        Option option_max_buffer = new Option("max", "max_buffer_size", true, "The maximum buffer size that stores the incoming events");
        Option option_buffer_size_coefficient = new Option("bc", "buffer_size_coefficient", true, "This is used to calculate the buffer size dynamically. The coefficient is multiplied with the number of events per minute.");
        Option option_verbose = new Option("v", "verbose", false, "If enabled, prints the parameter that are used");
        Option option_help = new Option("h", "help", false, "Show list of possible parameter");
        Option option_broker = new Option("r", "remote", false, "If disabled, 'localhost' is used as the IP for the broker");
        Option option_logging = new Option("l", "logging", false, "If enabled, writes performance logs into ./logs-worker.csv");

        // mark required options
//        option_training_data.setRequired(true);
//        option_test_data.setRequired(true);

        options
                .addOption(option_test_data)
                .addOption(option_min_buffer)
                .addOption(option_max_buffer)
                .addOption(option_buffer_size_coefficient)
                .addOption(option_verbose)
                .addOption(option_help)
                .addOption(option_broker)
                .addOption(option_logging);


        // parse the command line parameter
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ServerAppRunner", options);
            System.exit(0);
        }

        if (cmd.getArgs().length > 0) {
            // There are args that could not be associated with a existing parameter
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ServerAppRunner", options);
            System.exit(2);
        }

        // compute the arguments
        String testDataUrl = cmd.getOptionValue(CMD_TEST_DATA_URL, TEST_DATA_DEFAULT_URL);
        int minBufferSize = Integer.parseInt(cmd.getOptionValue("min_buffer_size", "128"));
        int maxBufferSize = Integer.parseInt(cmd.getOptionValue("max_buffer_size", "1024"));
        float bufferSizeCoefficient = Float.parseFloat(cmd.getOptionValue("buffer_size_coefficient", "0.3"));
        boolean verbose = cmd.hasOption("verbose");
        boolean isRemote = cmd.hasOption("remote");
        String broker = isRemote ? "kafka-0:9092" : "localhost:29092";
        boolean logToFile = cmd.hasOption("logging");

        // print used parameter
        if (verbose) {
            System.out.println();
            System.out.println("Used parameter:");
            System.out.println(String.format("    %s: %s", CMD_TEST_DATA_URL, testDataUrl));
            System.out.println("    min_buffer_size: " + minBufferSize);
            System.out.println("    max_buffer_size: " + maxBufferSize);
            System.out.println("    buffer_size_coefficient: " + bufferSizeCoefficient);
            System.out.println("    broker address: " + broker);
            System.out.println();
        }


        if (logToFile) {
            PrintStream fileOut = new PrintStream("./logs-worker.csv");
            System.setOut(fileOut);
            System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");
        }

        WorkerApp.downloadTestDatasetIfNecessary(testDataUrl);

        // Sleep in order to be sure that the ServerApp has already sent some training data
        Thread.sleep(10000);

        BaseKafkaApp.brokers = broker;
        WorkerApp worker = new WorkerApp(minBufferSize, maxBufferSize, bufferSizeCoefficient, TEST_DATA_FILE_PATH);

        try {
            worker.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

