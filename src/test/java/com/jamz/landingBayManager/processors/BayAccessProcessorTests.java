package com.jamz.landingBayManager.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jamz.landingBayManager.LandingBayManager;
import com.jamz.landingBayManager.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class BayAccessProcessorTests {

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

        // Construct test landing bay
        ObjectNode bay1 = new ObjectNode(nodeFactory);

        bay1.put("location", "123:456:-155");
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
    void testAccessRequest() {
        ObjectNode drone = new ObjectNode(nodeFactory);
        drone.put("drone_id", "0x1");
        drone.put("bay", "1");
        inputTopic.pipeInput("BayAccessRequest", drone);

        // First, make sure that theres something in the output topic. If there isn't, it'll throw an error when we try
        // to read from it.
        assertFalse(outputTopic.isEmpty());

        KeyValue<String, JsonNode> result = outputTopic.readKeyValue();
        assertEquals("BayAccessResponse", result.key);
        assertEquals("free", result.value.get("status").textValue());
        assertEquals("0x1", result.value.get("drone_id").textValue());

        // Make sure that the state store was updated accordingly
        JsonNode bay = bayStates.get("1");
        assertTrue(bay.get("in_use").booleanValue());
    }
}
