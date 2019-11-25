
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.hpi.MLForDataStreams

import java.util
import java.util.{Arrays, Locale, Properties}
import java.util.concurrent.CountDownLatch

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KTable, Produced, ValueMapper}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder, StreamsConfig}

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program
 * that computes a simple word occurrence histogram from an input text.
 * <p>
 * In this example, the input stream reads from a topic named "streams-plaintext-input", where the values of messages
 * represent lines of text; and the histogram output is written to topic "streams-wordcount-output" where each record
 * is an updated count of a single word.
 * <p>
 * Before running this example you must create the input topic and the output topic (e.g. via
 * {@code bin/kafka-topics.sh --create ...}), and write some data to the input topic (e.g. via
 * {@code bin/kafka-console-producer.sh}). Otherwise you won't see any data arriving in the output topic.
 */
final class WordCountDemo {

    val INPUT_TOPIC: String = "streams-plaintext-input"
    val OUTPUT_TOPIC: String = "streams-wordcount-output"

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
//          .count();
//
//        // need to override value serde to Long type
//        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));


        // TODO: convert to Scala Code
        val source: KStream[String, String] = builder.stream(INPUT_TOPIC)

        val counts = source
          .flatMapValues((value: String) => util.Arrays.asList(value.toLowerCase(Locale.getDefault).split(" ")))
          .groupBy((key: String, value: String) => value)
          .count

        // need to override value serde to Long type
        counts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String, Serdes.Long))
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

object WordCountDemo {

    def apply(): Unit = {
        new  WordCountDemo().run()
    }
}