#!/bin/bash

trap killgroup SIGINT

killgroup(){
  echo killing...
  kill 0
}

runWorker() {
  java -cp build/libs/kafka-ps-all.jar de.hpi.datastreams.apps.WorkerAppRunner -l
}

runServer() {
  sleep 10s
  java -cp build/libs/kafka-ps-all.jar de.hpi.datastreams.apps.ServerAppRunner -l -p 200
}

runWorker & runServer & wait