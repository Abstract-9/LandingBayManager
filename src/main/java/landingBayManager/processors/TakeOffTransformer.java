package landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class TakeOffTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {

    private ProcessorContext context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;

        this.bayStates = processorContext.getStateStore("bayStates");

    }

    @Override
    public KeyValue<String, JsonNode> transform(String s, JsonNode jsonNode) {
        //TODO implement bay access queue

        JsonNode bay = this.bayStates.get(jsonNode.get("bay").textValue());

        if (bay.get("in_use").booleanValue()) {
            ObjectNode response = jsonNode.deepCopy();
            response.put("status", "busy");
            return new KeyValue<>("TakeOffResponse", response);
        } else {
            ObjectNode bayWr = bay.deepCopy();
            bayWr.put("in_use", true);
            this.bayStates.put(jsonNode.get("bay").textValue(), bayWr);

            ObjectNode response = jsonNode.deepCopy();
            response.put("status", "free");
            return new KeyValue<>("TakeOffResponse", response);
        }
    }

    @Override
    public void close() {

    }
}
