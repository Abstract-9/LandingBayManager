package landingBayManager.transformers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class BayAssignmentTransformer implements Transformer<String, JsonNode, KeyValue<String, JsonNode>> {

    private ProcessorContext context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
        this.bayStates = processorContext.getStateStore("BayStates");
    }

    @Override
    public KeyValue<String, JsonNode> transform(String s, JsonNode jsonNode) {
        //TODO: Figure out closest available landing bay
        // For now, I'll just use any available

        KeyValueIterator<String, JsonNode> iter = this.bayStates.all();

        while(iter.hasNext()) {
            KeyValue<String, JsonNode> bay = iter.next();
            JsonNode bayStatus = bay.value;
            if (bayStatus.get("occupied_bays").intValue() < bayStatus.get("bay_count").intValue()) {
                ObjectNode newStatus = bayStatus.deepCopy();
                newStatus.put("occupied_bays", bayStatus.get("occupied_bays").intValue() + 1);
                this.bayStates.put(bay.key, newStatus);
                newStatus.remove("occupied_bays");
                newStatus.remove("bay_count");
                newStatus.put("status", "success");
                newStatus.put("drone_id", jsonNode.get("drone_id").textValue());
                return new KeyValue<>("BayAssignment", newStatus);
            }
        }

        ObjectNode resultStatus = jsonNode.deepCopy();
        resultStatus.removeAll();
        resultStatus.put("drone_id", jsonNode.get("drone_id").textValue());
        resultStatus.put("status", "error");
        return new KeyValue<>("BayAssignment", resultStatus);
    }

    @Override
    public void close() {

    }
}
