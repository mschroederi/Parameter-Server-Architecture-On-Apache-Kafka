package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URL;


class WorkerAppRunner {
    public static void main(String[] args) throws InterruptedException {
        /*
        String testDataFilePath = "./local_test_data.csv";
        URL url = new URL("https://s3.eu-central-1.amazonaws.com/de.hpi.datastreams.parameter-server/small-test.csv");
        File trainingsData = new File(testDataFilePath);
        FileUtils.copyURLToFile(url, trainingsData);
         */

        // PrintStream fileOut = new PrintStream("./log_worker.csv");
        // System.setOut(fileOut);

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");

        Thread.sleep(5000);

        WorkerApp worker = new WorkerApp(128, 1024);


        try {
            worker.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

