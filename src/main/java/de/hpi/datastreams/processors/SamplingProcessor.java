package de.hpi.datastreams.processors;

import de.hpi.datastreams.apps.SamplingApp;
import de.hpi.datastreams.messages.Message;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class SamplingProcessor extends AbstractProcessor<Long, Message> {

    private KeyValueStore<Long, Message> inputDataBuffer;
    long bufferSize;
    long currentIndex;

    public SamplingProcessor(long bufferSize){
        this.bufferSize = bufferSize;
        this.currentIndex = 0;
    }

    @Override
    public void init(ProcessorContext context) {
        super.init(context);
        this.inputDataBuffer = (KeyValueStore<Long, Message>) context.getStateStore(SamplingApp.INPUT_DATA_BUFFER);
    }

    @Override
    public void process(Long partitionKey, Message inputDataMessage) {
        // Insert message into KeyValueStore
        // Potentially overwrite the oldest meesage
        this.inputDataBuffer.put(this.currentIndex, inputDataMessage);

        // Debug output
        System.out.println("currentIndex: " + this.currentIndex);
        System.out.println("current message: " + inputDataMessage.toString());

        // update currentIndex
        this.currentIndex++;
        if(this.currentIndex >= this.bufferSize){
            this.currentIndex = 0;
        }
    }
}
