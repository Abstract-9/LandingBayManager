package landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class TakeOffProcessor implements Processor<String, JsonNode> {
    @Override
    public void init(ProcessorContext processorContext) {

    }

    @Override
    public void process(String s, JsonNode jsonNode) {

    }

    @Override
    public void close() {

    }
}
