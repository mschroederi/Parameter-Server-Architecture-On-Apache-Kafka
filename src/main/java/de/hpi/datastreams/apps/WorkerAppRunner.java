package de.hpi.datastreams.apps;

import de.hpi.datastreams.producer.CsvProducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;


class WorkerAppRunner {
    public static void main(String[] args) throws FileNotFoundException {

        PrintStream fileOut = new PrintStream("./log_worker.csv");
        System.setOut(fileOut);

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");

        // Turn off logging
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        WorkerApp worker = new WorkerApp(128, 1024);


        try {
            worker.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

