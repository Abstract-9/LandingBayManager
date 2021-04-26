package landingBayManager.transformers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class BayAccessTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {

    private ProcessorContext context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;

        this.bayStates = processorContext.getStateStore("bayStates");

    }

    @Override
    public KeyValue<String, JsonNode> transform(String s, JsonNode jsonNode) {

        JsonNode bay = this.bayStates.get(jsonNode.get("bay").textValue());
        ObjectNode bayWr = bay.deepCopy();
        ObjectNode response = jsonNode.deepCopy();

        if (bay.get("in_use").booleanValue()) {
            // Put this drone in the queue
            ((ArrayNode) bayWr.get("queue")).add(jsonNode.get("drone_id").textValue());
            this.bayStates.put(jsonNode.get("bay").textValue(), bayWr);

            // Now, return the response. Drone will wait until we tell it that it can access the bay.
            response.put("status", "busy");
        } else {
            bayWr.put("in_use", true);
            this.bayStates.put(jsonNode.get("bay").textValue(), bayWr);

            response.put("status", "free");
        }

        return new KeyValue<>("AccessResponse", response);
    }


    @Override
    public void close() {

    }
}
