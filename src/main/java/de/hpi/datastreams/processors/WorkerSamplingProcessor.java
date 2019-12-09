package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.App;
import de.hpi.datastreams.messages.DataMessage;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class WorkerSamplingProcessor extends AbstractProcessor<Long, DataMessage> {

    private KeyValueStore<Long, DataMessage> inputDataBuffer;
    private long bufferSize;
    private long currentIndex;

    public WorkerSamplingProcessor(long bufferSize) {
        this.bufferSize = bufferSize;
        this.currentIndex = 0;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.inputDataBuffer = (KeyValueStore<Long, DataMessage>) context.getStateStore(App.INPUT_DATA_BUFFER);
    }

    @Override
    public void process(Long partitionKey, DataMessage inputDataMessage) {
        // Insert message into KeyValueStore as RingBuffer
        // Potentially overwrite the oldest message
        this.inputDataBuffer.put(this.currentIndex, inputDataMessage);

        // Debug output
        System.out.println("SamplingProcessor - received message " + this.currentIndex);
//        System.out.println("current message: " + inputDataMessage.toString());

        // update currentIndex
        this.currentIndex++;
        if (this.currentIndex >= this.bufferSize) {
            this.currentIndex = 0;
        }
    }
}
