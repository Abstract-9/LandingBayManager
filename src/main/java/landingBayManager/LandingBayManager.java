/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package landingBayManager;

import com.fasterxml.jackson.databind.JsonNode;
import landingBayManager.processors.BayAccessProcessor;
import landingBayManager.processors.BayAssignmentProcessor;
import landingBayManager.processors.BayClearedProcessor;
import landingBayManager.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class LandingBayManager {

    static final String MAIN_TOPIC = "LandingBayManager";
    static final String BAY_STORE_TOPIC = "LandingBays";
    static final String BAY_STORE = "BayStates";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-landing-bay-manager");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "confluent:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);

        final Topology topology = buildTopology(props, false);
        run_streams(topology, props);
    }

    public static Topology buildTopology(Properties props, boolean testing) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final Consumed<String, JsonNode> consumed = Consumed.with(Serdes.String(), jsonSerde);

        final Topology topBuilder = new Topology();



        topBuilder.addSource("Main-Topic", MAIN_TOPIC);

        topBuilder
                .addProcessor("BayAssignment", BayAssignmentProcessor::new, "Main-Topic")
                .addProcessor("BayAccess", BayAccessProcessor::new, "Main-Topic")
                .addProcessor("BayCleared", BayClearedProcessor::new, "Main-Topic");

        if (!testing) {
            topBuilder.addGlobalStore(
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("BayStore"), Serdes.String(), jsonSerde),
                    "baySource",
                    Serdes.String().deserializer(),
                    jsonDeserializer,
                    BAY_STORE_TOPIC,
                    "Bay-State-Updater",
                    () -> new GlobalStoreUpdater<>("BayStore")
            );
        } else {
            topBuilder.addGlobalStore(
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("BayStore"), Serdes.String(), jsonSerde).withLoggingDisabled(),
                    "baySource",
                    Serdes.String().deserializer(),
                    jsonDeserializer,
                    BAY_STORE_TOPIC,
                    "Bay-State-Updater",
                    () -> new GlobalStoreUpdater<>("BayStore")
            );
        }

        topBuilder.addSink("Main-Output", "LandingBayManager", "BayAccess", "BayAssignment", "BayCleared")
                .addSink("BayState", BAY_STORE_TOPIC, "BayAccess", "BayAssignment", "BayCleared");

        return topBuilder;
    }

    private static void run_streams(Topology topology, Properties props) {
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }




    // Processor that keeps the global store updated.
    private static class GlobalStoreUpdater<K, V, KIn, KOut> implements Processor<K, V, KIn, KOut> {

        private final String storeName;

        public GlobalStoreUpdater(final String storeName) {
            this.storeName = storeName;
        }

        private KeyValueStore<K, V> store;

        @Override
        public void init(final ProcessorContext<KIn, KOut> processorContext) {
            store = processorContext.getStateStore(storeName);
        }

        @Override
        public void process(final Record<K, V> record) {
            // We are only supposed to put operation the keep the store updated.
            // We should not filter record or modify the key or value
            // Doing so would break fault-tolerance.
            // see https://issues.apache.org/jira/browse/KAFKA-7663
            store.put(record.key(), record.value());
        }

        @Override
        public void close() {
            // No-op
        }

    }
}

