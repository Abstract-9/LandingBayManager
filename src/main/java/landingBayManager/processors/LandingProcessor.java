package landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;

public class LandingProcessor implements Processor<String, JsonNode> {

    private ProcessorContext context;
    private TimestampedKeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;

        this.bayStates = processorContext.getStateStore("bayStates");

    }

    @Override
    public void process(String s, JsonNode jsonNode) {

    }

    @Override
    public void close() {

    }
}
