package com.jamz.landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.landingBayManager.serdes.JSONSerde;
import com.jamz.landingBayManager.util.Location;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import com.jamz.landingBayManager.LandingBayManager;

import static org.junit.jupiter.api.Assertions.*;

public class BayAssignmentProcessorTests {

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, JsonNode> inputTopic;
    private static TestOutputTopic<String, JsonNode> outputTopic;
    private static KeyValueStore<String, JsonNode> bayStates;
    private static final JsonNodeFactory nodeFactory = new JsonNodeFactory(true);


    @BeforeAll
    static void setup() {

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-landing-bay-manager");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        // Build the topology using the prod code.
        Topology topology = LandingBayManager.buildTopology(props, true);

        testDriver = new TopologyTestDriver(topology, props);

        inputTopic = testDriver.createInputTopic("LandingBayManager",
                Serdes.String().serializer(), jsonSerializer);
        outputTopic = testDriver.createOutputTopic("LandingBayManager",
                Serdes.String().deserializer(), jsonDeserializer);

        bayStates = testDriver.getKeyValueStore("BayStore");
    }

    @BeforeEach
    void populateStore() {
        // Construct test landing bay
        ObjectNode bay1 = new ObjectNode(nodeFactory);

        bay1.putObject("geometry");
        ((ObjectNode)bay1.get("geometry")).put("latitude", 45.420330);
        ((ObjectNode)bay1.get("geometry")).put("longitude", -75.680572);
        ((ObjectNode)bay1.get("geometry")).put("altitude", 0);
        bay1.put("bay_count", 2);
        bay1.put("occupied_bays", 0);
        bay1.put("in_use", false);
        bay1.putArray("queue");

        bayStates.put("1", bay1);
    }

    @AfterAll
    static void tearDown() {
        testDriver.close();
    }

    @Test
    void testAssignmentRequest() {
        inputTopic.pipeInput("BayAssignmentRequest",
                construct_drone("0x1", new Location(45.2, -75.7, 10)));

        // First, make sure that theres something in the output topic. If there isn't, it'll throw an error when we try
        // to read from it.
        assertFalse(outputTopic.isEmpty());
        // Read the output event and make sure that it matches the expected results
        KeyValue<String, JsonNode> result = outputTopic.readKeyValue();
        assertEquals("BayAssignment", result.key);
        assertEquals("success", result.value.get("status").textValue());
        assertEquals("0x1", result.value.get("drone_id").textValue());

        // Check the state store and ensure that it was also updated properly
        JsonNode bay = bayStates.get("1");
        assertEquals(1, bay.get("occupied_bays").intValue());
        assertFalse(bay.get("in_use").booleanValue());
    }

    @Test
    void testBayFull() {
        inputTopic.pipeInput("BayAssignmentRequest",
                construct_drone("0x1", new Location(45.2, -75.7, 10)));

        assertFalse(outputTopic.isEmpty());
        KeyValue<String, JsonNode> result = outputTopic.readKeyValue();
        assertEquals("BayAssignment", result.key);
        assertEquals("success", result.value.get("status").textValue());
        assertEquals("0x1", result.value.get("drone_id").textValue());

        JsonNode bay = bayStates.get("1");
        assertEquals(1, bay.get("occupied_bays").intValue());
        assertFalse(bay.get("in_use").booleanValue());

        inputTopic.pipeInput("BayAssignmentRequest",
                construct_drone("0x2", new Location(45.3, -75.8, 10)));

        assertFalse(outputTopic.isEmpty());
        result = outputTopic.readKeyValue();
        assertEquals("BayAssignment", result.key);
        assertEquals("success", result.value.get("status").textValue());
        assertEquals("0x2", result.value.get("drone_id").textValue());

        bay = bayStates.get("1");
        assertEquals(2, bay.get("occupied_bays").intValue());
        assertFalse(bay.get("in_use").booleanValue());

        inputTopic.pipeInput("BayAssignmentRequest",
                construct_drone("0x3", new Location(45.38, -75.67, 10)));

        assertFalse(outputTopic.isEmpty());
        result = outputTopic.readKeyValue();
        assertEquals("BayAssignment", result.key);
        assertEquals("error", result.value.get("status").textValue());
        assertEquals("0x3", result.value.get("drone_id").textValue());

        bay = bayStates.get("1");
        assertEquals(2, bay.get("occupied_bays").intValue());
        assertFalse(bay.get("in_use").booleanValue());
    }

    @Test
    void testClosestBay() {

        // Construct a second landing bay
        ObjectNode bay2 = new ObjectNode(nodeFactory);

        bay2.putObject("geometry");
        ((ObjectNode)bay2.get("geometry")).put("latitude", 45.424150);
        ((ObjectNode)bay2.get("geometry")).put("longitude", -75.686422);
        ((ObjectNode)bay2.get("geometry")).put("altitude", 0);
        bay2.put("bay_count", 2);
        bay2.put("occupied_bays", 0);
        bay2.put("in_use", false);
        bay2.putArray("queue");

        bayStates.put("2", bay2);

        // This drone is closer to landing bay 2, so it should get assigned there
        inputTopic.pipeInput("BayAssignmentRequest",
                construct_drone("0x1", new Location(45.42718, -75.694394, 10)));

        assertFalse(outputTopic.isEmpty());
        KeyValue<String, JsonNode> result = outputTopic.readKeyValue();
        assertEquals("BayAssignment", result.key);
        assertEquals("success", result.value.get("status").textValue());
        assertEquals("0x1", result.value.get("drone_id").textValue());

        JsonNode bay = bayStates.get("2");
        assertEquals(1, bay.get("occupied_bays").intValue());
        assertFalse(bay.get("in_use").booleanValue());
        bay = bayStates.get("1");
        assertEquals(0, bay.get("occupied_bays").intValue());
        assertFalse(bay.get("in_use").booleanValue());
    }

    ObjectNode construct_drone(String id, Location location) {
        ObjectNode drone = new ObjectNode(nodeFactory);
        drone.put("drone_id", id);
        drone.put("latitude", location.latitude);
        drone.put("longitude", location.longitude);
        drone.put("altitude", location.altitude);
        return drone;
    }

}
