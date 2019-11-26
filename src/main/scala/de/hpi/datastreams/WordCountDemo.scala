package de.hpi.datastreams

import java.util
import java.util.concurrent.CountDownLatch
import java.util.{Locale, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, Materialized, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}

class WordCountDemo {

    val INPUT_TOPIC: String = "streams-plaintext-input"
    val OUTPUT_TOPIC: String = "streams-wordcount-output"
    val STORE_NAME: String = "CountsKeyValueStore"

    def getStreamsConfig: Properties = {
        val props: Properties = new Properties()
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount")
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        props
    }

    def createWordCountStream(builder: StreamsBuilder): Unit = {

        //        JAVA Code:
        //        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        //
        //        final KTable<String, Long> counts = source
        //          .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
        //          .groupBy((key, value) -> value)
        //          .count(Materialized.as(STORE_NAME));
        //
        //        // need to override value serde to Long type
        //        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        //        // TODO: convert to Scala Code
        //        val source: KStream[String, String] = builder.stream(INPUT_TOPIC)
        //
        //        val counts = source
        //          .flatMapValues((value: String) => util.Arrays.asList(value.toLowerCase(Locale.getDefault).split(" ")))
        //          .groupBy((key: String, value: String) => value)
        //          .count(Materialized.as(STORE_NAME))
        //
        //        // need to override value serde to Long type
        //        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String, Serdes.Long))
    }

    def run(): Unit = {
        val props: Properties = getStreamsConfig

        val builder: StreamsBuilder = new StreamsBuilder()
        createWordCountStream(builder)
        val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
        val latch: CountDownLatch = new CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime.addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            override def run(): Unit = {
                streams.close()
                latch.countDown()
            }
        })

        try {
            streams.start()
            latch.await()
        }
        catch {
            case Throwable => System.exit(1)
        }
        System.exit(0)
    }
}