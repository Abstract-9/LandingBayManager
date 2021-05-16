package com.jamz.landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.jamz.landingBayManager.LandingBayManager.Constants;

public class BayAccessProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext<String, JsonNode> processorContext) {
        this.context = processorContext;

        this.bayStates = processorContext.getStateStore(Constants.BAY_STORE_NAME);

    }

    @Override
    public void process(Record<String, JsonNode> record) {
        if (!record.key().equals("BayAccessRequest") && !record.key().equals("BayAccessUpdate")) return;

        JsonNode jsonNode = record.value();

        JsonNode bay = this.bayStates.get(jsonNode.get("bay").textValue());
        ObjectNode bayWr = bay.deepCopy();
        ObjectNode response = jsonNode.deepCopy();

        if (bay.get("in_use").booleanValue()) {
            // Put this drone in the queue
            ((ArrayNode) bayWr.get("queue")).add(jsonNode.get("drone_id").textValue());
            this.context.forward(
                    new Record<String, JsonNode>(
                            jsonNode.get("bay").textValue(), bayWr, System.currentTimeMillis()), "BayState"
            );

            // Now, return the response. Drone will wait until we tell it that it can access the bay.
            response.put("status", "busy");
        } else {
            bayWr.put("in_use", true);
            this.context.forward(
                    new Record<String, JsonNode>(jsonNode.get("bay").textValue(), bayWr, System.currentTimeMillis()),
                    Constants.STORE_OUTPUT_NAME
            );

            response.put("status", "free");
        }

        this.context.forward(
                new Record<>("BayAccessResponse", response, System.currentTimeMillis()),
                Constants.MAIN_OUTPUT_NAME
        );
    }


    @Override
    public void close() {

    }
}
