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

public class WorkerSamplingProcessor extends AbstractProcessor<Long, LabeledData> {

    private KeyValueStore<Long, MyArrayList<LabeledDataWithAge>> inputDataBuffer;
    private long bufferSize;

    public WorkerSamplingProcessor(long bufferSize) {
        this.bufferSize = bufferSize;
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
        KeyValueIterator<Long, MyArrayList<LabeledDataWithAge>> iterator =
                this.inputDataBuffer.range(partitionKey, partitionKey);

        // Buffer was already initialized
        if (iterator.hasNext()) {
            KeyValue<Long, MyArrayList<LabeledDataWithAge>> data = iterator.next();

            // Remove first element of ring buffer
            // if the buffer will exceed the maximum allowed buffer size
            if (data.value.size() >= this.bufferSize) {
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
}
