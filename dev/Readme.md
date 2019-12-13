# Information for Developer

## Requirements
- Please have [Docker](https://docs.docker.com/install/) installed.

## Start a Kafka cluster
First, make sure to delete all old volumes by executing
```$bash
docker-compose down -V
```
Then start the cluster by running
```$bash
docker-compose up
```

You might want to access Confluent's cluster UI at [http://0.0.0.0:9021/](http://0.0.0.0:9021/).

## Run the Parameter Server
After you completed the above feel free to start the Parameter Server through your IDE or by building a Jar and running that from terminal.