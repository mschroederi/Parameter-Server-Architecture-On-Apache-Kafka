package de.hpi.datastreams.apps;

import java.io.IOException;


class WorkerAppRunner {

    public static void main(String[] args) throws InterruptedException, IOException {

        WorkerApp.downloadDatasetsIfNecessary();

        // Sleep in order to be sure that the ServerApp has already sent some training data
        Thread.sleep(10000);

        System.out.println("timestamp;partition;vectorClock;loss;fMeasure;accuracy");
        WorkerApp worker = new WorkerApp(128, 1024);

        try {
            worker.call();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

