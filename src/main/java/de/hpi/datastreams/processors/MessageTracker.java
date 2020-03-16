package de.hpi.datastreams.processors;

import lombok.Getter;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

class MessageStatus {

    @Getter
    private Integer vectorClock;
    @Getter
    private Boolean weightsMessageSent;

    MessageStatus(Integer vectorClock, Boolean weightsMessageSent) {
        this.vectorClock = vectorClock;
        this.weightsMessageSent = weightsMessageSent;
    }

    void sentMessage(Integer vectorClock) {
        if (!this.vectorClock.equals(vectorClock)) {
            throw new IllegalArgumentException(String.format("Expected value %d, actual value %d", this.vectorClock, vectorClock));
        }
        this.weightsMessageSent = true;
    }

    void receivedMessage(Integer vectorClock) {
        if (!this.vectorClock.equals(vectorClock)) {
            throw new IllegalArgumentException(String.format("Expected value %d, actual value %d", this.vectorClock, vectorClock));
        }
        this.vectorClock++;
        this.weightsMessageSent = false;
    }

    Boolean wasWeightsMessageSent() {
        return this.weightsMessageSent;
    }
}

class MessageTracker {

    Integer numWorkers;
    ArrayList<MessageStatus> tracker = new ArrayList<>();

    MessageTracker(Integer numWorkers) {
        this.numWorkers = numWorkers;

        for (int i = 0; i < this.numWorkers; i++) {
            this.tracker.add(new MessageStatus(0, true));
        }
    }

    void receivedMessage(Long partitionKey, Integer vectorClock) {
        this.tracker.get(partitionKey.intValue()).receivedMessage(vectorClock);
    }

    void sentMessage(Long partitionKey, Integer vectorClock) {
        this.tracker.get(partitionKey.intValue()).sentMessage(vectorClock);
    }

    void sentAllMessages(Integer vectorClock) {
        for (long partitionKey = 0; partitionKey < this.numWorkers; partitionKey++) {
            this.sentMessage(partitionKey, vectorClock);
        }
    }

    ArrayList<Tuple2<Long, Integer>> getAllSendableMessages(Integer receivedVectorClock, Integer maxDelay) {
        ArrayList<Tuple2<Long, Integer>> sendableMessages = new ArrayList<>();
        IntStream.range(0, this.numWorkers)
                .mapToObj(partitionKey ->
                        new Tuple2<>((long) partitionKey, this.tracker.get(partitionKey)))
                .filter(tuple -> !tuple._2.wasWeightsMessageSent())
                .filter(tuple -> this.hasReceivedAllMessages(tuple._2.getVectorClock() - maxDelay - 1))
                .map(tuple -> new Tuple2<>(tuple._1, tuple._2.getVectorClock()))
                .forEach(sendableMessages::add);
        return sendableMessages;
    }

    Boolean hasReceivedAllMessages(Integer vectorClock) {
        int minVC = this.tracker.stream()
                .map(MessageStatus::getVectorClock)
                .mapToInt(v -> v)
                .min().orElseThrow(NoSuchElementException::new);
        return minVC >= vectorClock + 1;
    }
}
