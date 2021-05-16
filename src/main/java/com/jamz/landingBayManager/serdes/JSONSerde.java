package com.jamz.landingBayManager.serdes;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;

public final class JSONSerde extends Serdes.WrapperSerde<JsonNode> {
    public JSONSerde() {
        super(new JsonSerializer(), new JsonDeserializer());
    }
}
