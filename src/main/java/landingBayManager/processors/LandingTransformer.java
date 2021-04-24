package landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class LandingTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {

    private ProcessorContext context;
    private TimestampedKeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;

        this.bayStates = processorContext.getStateStore("bayStates");

    }

    @Override
    public KeyValue<String, JsonNode> transform(String s, JsonNode jsonNode) {

    }


    @Override
    public void close() {

    }
}
