package landingBayManager.transformers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class BayClearedTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {

    private ProcessorContext context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.bayStates = processorContext.getStateStore("BayStates");
    }

    @Override
    public KeyValue<String, JsonNode> transform(String s, JsonNode jsonNode) {

        JsonNode bay = this.bayStates.get(jsonNode.get("bay").textValue());
        ObjectNode bayWr = bay.deepCopy();
        ObjectNode response = jsonNode.deepCopy();

        ArrayNode bayQueue = (ArrayNode) bay.get("queue");

        if (bayQueue.isEmpty()) {
            bayWr.put("in_use", false);
            this.bayStates.put(jsonNode.get("bay").textValue(), bayWr);
        } else {
            String nextInLine = bayQueue.get(0).textValue();
            bayQueue.remove(0);
            response.put("drone_id", nextInLine);
            response.put("status", "free");
            this.context.forward("AccessResponse", response);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
