package landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class BayClearedProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext<String, JsonNode> processorContext) {
        this.context = processorContext;
        this.bayStates = processorContext.getStateStore("BayStore");
    }

    @Override
    public void process(Record<String, JsonNode> record) {
        if (!record.key().equals("BayCleared")) return;
        JsonNode value = record.value();

        JsonNode bay = this.bayStates.get(value.get("bay").textValue());
        ObjectNode bayWr = bay.deepCopy();
        ObjectNode response = value.deepCopy();

        ArrayNode bayQueue = (ArrayNode) bay.get("queue");

        if (bayQueue.isEmpty()) {
            bayWr.put("in_use", false);
            this.bayStates.put(value.get("bay").textValue(), bayWr);
        } else {
            String nextInLine = bayQueue.get(0).textValue();
            bayQueue.remove(0);
            response.put("drone_id", nextInLine);
            response.put("status", "free");
            this.context.forward(
                    new Record<String, JsonNode>("AccessResponse", response, System.currentTimeMillis()),
                    "Main-Output"
            );
        }
    }

    @Override
    public void close() {

    }
}
