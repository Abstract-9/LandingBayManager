package com.jamz.landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.landingBayManager.util.Location;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import com.jamz.landingBayManager.LandingBayManager.Constants;

import java.util.ArrayList;

public class BayAssignmentProcessor implements Processor<String, JsonNode, String, JsonNode> {

    private ProcessorContext<String, JsonNode> context;
    private KeyValueStore<String, JsonNode> bayStates;

    @Override
    public void init(ProcessorContext<String, JsonNode> processorContext) {
        this.context = processorContext;
        this.bayStates = processorContext.getStateStore(Constants.BAY_STORE_NAME);
    }

    //TODO distance-biased decision making implemented. Gotta make tests.
    @Override
    public void process(Record<String, JsonNode> record) {
        if (!record.key().equals("BayAssignmentRequest")) return;

        boolean bayAssigned = false;

        JsonNode jsonNode = record.value();

        KeyValueIterator<String, JsonNode> iter = this.bayStates.all();

        // The bays available for any one drone shouldn't be too big, so for now we can just sort them all into an array.
        // THIS WILL NEED TO BE MORE EFFICIENT IF DRONES HAVE ACCESS TO MANY, MANY LANDING BAYS
        ArrayList<KeyValue<String, JsonNode>> bayList = new ArrayList<>();
        iter.forEachRemaining(bayList::add);

        Location droneLocation = new Location(
                jsonNode.get("latitude").doubleValue(),
                jsonNode.get("longitude").doubleValue(),
                jsonNode.get("altitude").doubleValue()
        );

        // Sort the landing bays by distance
        bayList.sort((KeyValue<String, JsonNode> bay1, KeyValue<String, JsonNode> bay2) -> {
            Location loc1 = new Location(
                    bay1.value.get("geometry").get("latitude").doubleValue(),
                    bay1.value.get("geometry").get("longitude").doubleValue(),
                    0);
            Location loc2 = new Location(
                    bay2.value.get("geometry").get("latitude").doubleValue(),
                    bay2.value.get("geometry").get("longitude").doubleValue(),
                    0);

            return (int) Math.floor(droneLocation.getDistanceTo(loc1)) - (int) Math.floor(droneLocation.getDistanceTo(loc2));
        });

        // Now that the list is sorted, we can just go through in order.
        for (KeyValue<String, JsonNode> bay : bayList) {

            JsonNode bayStatus = bay.value;

            // Check if the bay in question has free space
            if (bayStatus.get("occupied_bays").intValue() < bayStatus.get("bay_count").intValue()) {
                ObjectNode newStatus = bayStatus.deepCopy();
                newStatus.put("occupied_bays", bayStatus.get("occupied_bays").intValue() + 1);

                // We're taking a space, so we'll update the bay by forwarding the new status to the bay state Topic
                this.context.forward(
                        new Record<String, JsonNode>(bay.key, newStatus, System.currentTimeMillis()),
                        Constants.STORE_OUTPUT_NAME
                );
                newStatus.remove("occupied_bays");
                newStatus.remove("bay_count");
                newStatus.put("status", "success");
                newStatus.put("drone_id", jsonNode.get("drone_id").textValue());
                this.context.forward(
                        new Record<String, JsonNode>("BayAssignment", newStatus, System.currentTimeMillis()),
                        Constants.MAIN_OUTPUT_NAME
                );
                bayAssigned = true;
                break;
            }
        }

        if (!bayAssigned) {
            ObjectNode resultStatus = jsonNode.deepCopy();
            resultStatus.removeAll();
            resultStatus.put("drone_id", jsonNode.get("drone_id").textValue());
            resultStatus.put("status", "error");
            this.context.forward(
                    new Record<String, JsonNode>("BayAssignment", resultStatus, System.currentTimeMillis()),
                    Constants.MAIN_OUTPUT_NAME
            );
        }
    }

    @Override
    public void close() {

    }
}
