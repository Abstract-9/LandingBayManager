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
        if (!record.value().get("eventType").textValue().equals("AccessRequest") &&
            !record.value().get("eventType").textValue().equals("AccessComplete")) return;

        JsonNode jsonNode = record.value();

        // Sanity check
        if (!jsonNode.has("bay_id") || !jsonNode.get("bay_id").isTextual()) return;
        JsonNode bay = this.bayStates.get(jsonNode.get("bay_id").textValue());
        ObjectNode bayWr = bay.deepCopy();
        ObjectNode response = jsonNode.deepCopy();

        if (jsonNode.get("eventType").textValue().equals("AccessRequest")) {
            if (bay.get("in_use").booleanValue()) {
                // Put this drone in the queue
                ((ArrayNode) bayWr.get("queue")).add(record.key());
                this.context.forward(
                        new Record<String, JsonNode>(
                                jsonNode.get("bay_id").textValue(), bayWr, System.currentTimeMillis()), "BayState"
                );

                // Now, return the response. Drone will wait until we tell it that it can access the bay.
                response.put("eventType", "AccessQueued");
            } else {
                bayWr.put("in_use", true);
                this.context.forward(
                        new Record<String, JsonNode>(jsonNode.get("bay_id").textValue(), bayWr, System.currentTimeMillis()),
                        Constants.STORE_OUTPUT_NAME
                );

                response.put("eventType", "AccessGranted");
            }

            this.context.forward(
                    new Record<>(record.key(), response, System.currentTimeMillis()),
                    Constants.BAY_ACCESS_OUTPUT_NAME
            );
        } else {
            ArrayNode bayQueue = (ArrayNode) bay.get("queue");
            if (bayQueue.isEmpty()) {
                bayWr.put("in_use", false);
                this.context.forward(
                        new Record<String, JsonNode>(jsonNode.get("bay_id").textValue(), bayWr, System.currentTimeMillis()),
                        Constants.STORE_OUTPUT_NAME
                );
            } else {
                String nextInLine = bayQueue.get(0).textValue();
                bayQueue.remove(0);
                bayWr.set("queue", bayQueue);
                this.context.forward(
                        new Record<String, JsonNode>(jsonNode.get("bay_id").textValue(), bayWr, System.currentTimeMillis()),
                        Constants.STORE_OUTPUT_NAME
                );
                response.put("eventType", "AccessGranted");
                this.context.forward(
                        new Record<String, JsonNode>(nextInLine, response, System.currentTimeMillis()),
                        Constants.BAY_ACCESS_OUTPUT_NAME
                );
            }
        }
    }


    @Override
    public void close() {

    }
}
