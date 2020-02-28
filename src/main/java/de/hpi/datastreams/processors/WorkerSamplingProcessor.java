package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.App;
import de.hpi.datastreams.messages.LabeledData;
import de.hpi.datastreams.messages.LabeledDataWithAge;
import de.hpi.datastreams.messages.MyArrayList;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.LinkedList;
import java.util.OptionalDouble;

public class WorkerSamplingProcessor extends AbstractProcessor<Long, LabeledData> {

    private KeyValueStore<Long, MyArrayList<LabeledDataWithAge>> inputDataBuffer;
    private LinkedList<Long> processingTimes = new LinkedList<>();
    private long PROCESSING_INTERVAL_SIZE = 500;
    private Long lastProcessedTime = null;
    private long maxBufferSize;
    private long minBufferSize;

    public WorkerSamplingProcessor(long minBufferSize, long maxBufferSize) {
        this.minBufferSize = minBufferSize;
        this.maxBufferSize = maxBufferSize;
    }

    @Override
    @SuppressWarnings(value = "unchecked")
    public void init(ProcessorContext context) {
        super.init(context);
        this.inputDataBuffer = (KeyValueStore<Long, MyArrayList<LabeledDataWithAge>>) context.getStateStore(App.INPUT_DATA_BUFFER);
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
        this.handleNewProcessingTime();

        KeyValueIterator<Long, MyArrayList<LabeledDataWithAge>> iterator =
                this.inputDataBuffer.range(partitionKey, partitionKey);

        // Buffer was already initialized
        if (iterator.hasNext()) {
            KeyValue<Long, MyArrayList<LabeledDataWithAge>> data = iterator.next();

            // Remove first element of ring buffer
            // if the buffer will exceed the maximum allowed buffer size
            while (data.value.size() >= this.calculateCurrentBufferSize()) {
                data.value.remove(0);
            }

            // Add newest data record to buffer
            data.value.add(LabeledDataWithAge.from(labeledData.getInputData(), labeledData.getLabel()));
            this.inputDataBuffer.put(data.key, data.value);
        }
        // Initialize buffer if there is no entry for partitionKey
        else {
            MyArrayList<LabeledDataWithAge> data = new MyArrayList<>();
            data.add(LabeledDataWithAge.from(labeledData.getInputData(), labeledData.getLabel()));

            // Store in KeyValueStore
            this.inputDataBuffer.put(partitionKey, data);
        }
    }

    public long calculateCurrentBufferSize(){
        OptionalDouble medianTimeDifference = this.processingTimes.stream().mapToLong(time -> time).average();
        double eventsPerMinute = 60000 / medianTimeDifference.orElse(1);
        //System.out.println("Events per minute: " + eventsPerMinute);

        // This can be modified in order to use a different function for describing the relationship between number of events and buffer size
        long calculatedBufferSize = Math.round(0.3 * eventsPerMinute);
        //System.out.println("buffer size: " + Math.max(this.minBufferSize, Math.min(this.maxBufferSize, calculatedBufferSize)));
        return Math.max(this.minBufferSize, Math.min(this.maxBufferSize, calculatedBufferSize));
    }

    public void handleNewProcessingTime(){
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
