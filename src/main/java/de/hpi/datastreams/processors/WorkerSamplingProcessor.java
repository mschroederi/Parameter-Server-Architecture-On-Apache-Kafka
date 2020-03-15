package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.WorkerApp;
import de.hpi.datastreams.messages.LabeledData;
import de.hpi.datastreams.messages.LabeledDataWithAge;
import javafx.util.Pair;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.math.LongRange;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;
import java.util.stream.Collectors;

public class WorkerSamplingProcessor extends AbstractProcessor<Long, LabeledData> {

    private KeyValueStore<Long, LabeledDataWithAge> inputDataBuffer;
    private LinkedList<Long> processingTimes = new LinkedList<>();
    private long PROCESSING_INTERVAL_SIZE = 500;
    private Long lastProcessedTime = null;
    private int maxBufferSize;
    private int minBufferSize;
    private float bufferSizeCoefficient;

    public WorkerSamplingProcessor(int minBufferSize, int maxBufferSize, float bufferSizeCoefficient) {
        this.minBufferSize = minBufferSize;
        this.maxBufferSize = maxBufferSize;
        this.bufferSizeCoefficient = bufferSizeCoefficient;
    }

    @Override
    @SuppressWarnings(value = "unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        this.inputDataBuffer = (KeyValueStore<Long, LabeledDataWithAge>)
                context.getStateStore(WorkerApp.INPUT_DATA_BUFFER);
    }

    /**
     * Inserts incoming message containing new data into the buffer with respect to the maximum allowed bufferSize.
     * Oldest message is overwritten if the buffer reached its maximum size.
     *
     * @param partitionKey Partition key of input topic
     * @param labeledData  DataMessage written to the INPUT_DATA stream
     */
    @Override
    public void process(Long partitionKey, LabeledData labeledData) {
//        System.out.println("WorkerSamplingProcessor - Received dataMessage on partition " + partitionKey);

        this.handleNewProcessingTime();

        int targetBufferSize = this.calculateCurrentBufferSize();

        long startOfDataKeySpace = partitionKey * this.maxBufferSize;
        long endOfDataKeySpace = startOfDataKeySpace + this.maxBufferSize - 1;
        KeyValueIterator<Long, LabeledDataWithAge> iterator =
                this.inputDataBuffer.range(startOfDataKeySpace, endOfDataKeySpace);


        List<Pair<Long, Long>> bufferIndex = new ArrayList<>();
        while (iterator.hasNext()) {
            KeyValue<Long, LabeledDataWithAge> data = iterator.next();
            bufferIndex.add(new Pair<>(data.key, data.value.getInsertionID()));
        }
        iterator.close();

        // Identify keys without assigned values
        HashSet<Long> emptyKeys = new HashSet<>(Arrays.asList(ArrayUtils.toObject(
                new LongRange(startOfDataKeySpace, endOfDataKeySpace).toArray())));
        emptyKeys.removeAll(bufferIndex.stream().map(Pair::getKey).collect(Collectors.toList()));

        long keyToOverwrite = -1L;
        long largestInsertionID = bufferIndex.stream()
                .map(Pair::getValue)
                .max(Comparator.comparing(Long::valueOf))
                .orElse(0L);

        // If the target buffer size is not yet met, start filling it up
        // by storing the received data tuple at the first empty key
        if (bufferIndex.size() < targetBufferSize) {
            // As the targetBufferSize cannot exceed the maximum allowed buffer size,
            // there are always empty keys if the buffer is not completely filled up
            keyToOverwrite = emptyKeys.stream().min(Long::compareTo).get();
        }
        // If the target buffer size is already met, just replace
        // the oldest data tuple with the received one
        else if (bufferIndex.size() == targetBufferSize) {
            keyToOverwrite = bufferIndex.stream()
                    .min(Comparator.comparing(Pair::getValue))
                    .get().getKey();
        }
        // If the buffer currently contains more data tuples than the target buffer size allows,
        // delete the n oldest data tuples and replace the (n-1)th oldest data tuple
        else if (bufferIndex.size() > targetBufferSize) {
            bufferIndex.sort(Comparator.comparing(Pair::getValue));
            int numTuplesToRemove = bufferIndex.size() - targetBufferSize;

            // Delete the n oldest data tuples
            for (int i = 0; i < numTuplesToRemove; i++) {
                long keyToRemove = bufferIndex.get(i).getKey();
                this.inputDataBuffer.delete(keyToRemove);
            }

            // Get (n-1)th data tuple ordered by insertionID
            keyToOverwrite = bufferIndex.get(numTuplesToRemove).getKey();
        }

        // Add newest data record to buffer
        LabeledDataWithAge newRecord = LabeledDataWithAge.from(labeledData.getInputData(),
                labeledData.getLabel(), largestInsertionID + 1);
        this.inputDataBuffer.put(keyToOverwrite, newRecord);
    }

    private int calculateCurrentBufferSize() {
        OptionalDouble meanTimeDifference = this.processingTimes.stream().mapToLong(time -> time).average();
        double eventsPerMinute = 60000 / meanTimeDifference.orElse(1000);

        // This can be modified in order to use a different function for describing the relationship between number of events and buffer size
        int calculatedBufferSize = (int) Math.round(this.bufferSizeCoefficient * eventsPerMinute);
        return Math.max(this.minBufferSize, Math.min(this.maxBufferSize, calculatedBufferSize));
    }

    private void handleNewProcessingTime() {
        if (this.lastProcessedTime == null) {
            this.lastProcessedTime = System.currentTimeMillis();
            return;
        }

        long currentTime = System.currentTimeMillis();
        this.processingTimes.addLast(currentTime - this.lastProcessedTime);
        this.lastProcessedTime = currentTime;
        if (this.processingTimes.size() > PROCESSING_INTERVAL_SIZE) // TODO: consider making this time dependent and not size dependent
            this.processingTimes.removeFirst();
    }
}
