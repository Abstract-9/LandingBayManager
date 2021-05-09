package landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;

import java.util.HashMap;

public class BayAssignmentProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext<String, JsonNode> processorContext) {
        this.context = processorContext;
        this.bayStates = processorContext.getStateStore("BayStore");

        // Setup serdes, because this is the only place I can do it?
        this.context.valueSerde().configure(new HashMap<String, JsonNode>(), false);

    }

    @Override
    public void process(Record<String, JsonNode> record) {
        if (!record.key().equals("BayAssignmentRequest")) return;
        //TODO: Figure out closest available landing bay
        // For now, I'll just use any available

        JsonNode jsonNode = record.value();

        KeyValueIterator<String, JsonNode> iter = this.bayStates.all();

        while(iter.hasNext()) {
            KeyValue<String, JsonNode> bay = iter.next();

            JsonNode bayStatus = bay.value;
            // Check if the bay in question has free space
            if (bayStatus.get("occupied_bays").intValue() < bayStatus.get("bay_count").intValue()) {
                ObjectNode newStatus = bayStatus.deepCopy();
                newStatus.put("occupied_bays", bayStatus.get("occupied_bays").intValue() + 1);
                // We're taking a space, so we'll update the bay by forwarding the new status to the bay state Topic
                this.context.forward(new Record<String, JsonNode>(bay.key, newStatus, System.currentTimeMillis()), "BayState");
                newStatus.remove("occupied_bays");
                newStatus.remove("bay_count");
                newStatus.put("status", "success");
                newStatus.put("drone_id", jsonNode.get("drone_id").textValue());
                this.context.forward(
                        new Record<String, JsonNode>("BayAssignment", newStatus, System.currentTimeMillis()),
                        "Main-Output"
                );
            }
        }

        ObjectNode resultStatus = jsonNode.deepCopy();
        resultStatus.removeAll();
        resultStatus.put("drone_id", jsonNode.get("drone_id").textValue());
        resultStatus.put("status", "error");
        this.context.forward(
                new Record<String, JsonNode>("BayAssignment", resultStatus, System.currentTimeMillis()),
                "Main-Output"
        );
    }

    @Override
    public void close() {

    }
}
