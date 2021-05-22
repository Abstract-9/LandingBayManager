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
package com.jamz.landingBayManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.jamz.landingBayManager.processors.BayAccessProcessor;
import com.jamz.landingBayManager.processors.BayAssignmentProcessor;
import com.jamz.landingBayManager.serdes.JSONSerde;
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



        topBuilder.addSource(Constants.BAY_ACCESS_TOPIC, Constants.BAY_ACCESS_TOPIC)
                .addSource(Constants.BAY_ASSIGNMENT_TOPIC, Constants.BAY_ASSIGNMENT_TOPIC);

        topBuilder
                .addProcessor(Constants.ASSIGNMENT_PROCESSOR_NAME, BayAssignmentProcessor::new, Constants.BAY_ASSIGNMENT_TOPIC)
                .addProcessor(Constants.ACCESS_PROCESSOR_NAME, BayAccessProcessor::new, Constants.BAY_ACCESS_TOPIC);



        if (!testing) {
            topBuilder.addGlobalStore(
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(Constants.BAY_STORE_NAME), Serdes.String(), jsonSerde),
                    "baySource",
                    Serdes.String().deserializer(),
                    jsonDeserializer,
                    Constants.BAY_STORE_TOPIC,
                    "Bay-State-Updater",
                    () -> new GlobalStoreUpdater<>(Constants.BAY_STORE_NAME)
            );
        } else {
            topBuilder.addGlobalStore(
                    Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(Constants.BAY_STORE_NAME), Serdes.String(), jsonSerde).withLoggingDisabled(),
                    "baySource",
                    Serdes.String().deserializer(),
                    jsonDeserializer,
                    Constants.BAY_STORE_TOPIC,
                    "Bay-State-Updater",
                    () -> new GlobalStoreUpdater<>(Constants.BAY_STORE_NAME)
            );
        }

        topBuilder.addSink(Constants.BAY_ACCESS_OUTPUT_NAME, Constants.BAY_ACCESS_TOPIC,
                        Constants.ACCESS_PROCESSOR_NAME)

                .addSink(Constants.BAY_ASSIGNMENT_OUTPUT_NAME, Constants.BAY_ASSIGNMENT_TOPIC,
                        Constants.ASSIGNMENT_PROCESSOR_NAME)

                .addSink(Constants.STORE_OUTPUT_NAME, Constants.BAY_STORE_TOPIC,
                        Constants.ACCESS_PROCESSOR_NAME, Constants.ASSIGNMENT_PROCESSOR_NAME);

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
    // From: https://github.com/confluentinc/kafka-streams-examples/blob/6.1.1-post/src/main/java/io/confluent/examples/streams/GlobalStoresExample.java
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

    // The kafka streams processor api uses a lot of internal names, and we need to refer to a few external topics.
    // This just helps keep everything in one place.
    public static class Constants {
        // Topics
        public static final String BAY_ASSIGNMENT_TOPIC = "BayAssignment";
        public static final String BAY_ACCESS_TOPIC = "BayAccess";
        public static final String BAY_STORE_TOPIC = "LandingBays";

        // Internal names
        public static final String BAY_ACCESS_OUTPUT_NAME = "AccessOutput";
        public static final String BAY_ASSIGNMENT_OUTPUT_NAME = "AssignmentOutput";
        public static final String BAY_STORE_NAME = "BayStore";
        public static final String STORE_OUTPUT_NAME = "BayState";
        public static final String ASSIGNMENT_PROCESSOR_NAME = "BayAssignmentProcessor";
        public static final String ACCESS_PROCESSOR_NAME = "BayAccessProcessor";

    }
}

