package com.trumid.cast.kafka.config;

import com.trumid.cast.kafka.serialize.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public final class KafkaProperties {
    public static final String BOOTSTRAP_SERVER = "localhost:9092";

    private static String applicationServer(int instanceId) {
        final int port = 9080 + instanceId;
        return "localhost:" + port;
    }

    public static Properties castSubProperties(String groupId, int instanceId) {
        final Properties properties = new Properties();
        properties.put(APPLICATION_SERVER_CONFIG, applicationServer(instanceId)); // don't need this here now
        properties.put(APPLICATION_ID_CONFIG, groupId + instanceId);
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, CastKeyDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, CastDeserializer.class);
        properties.put(GROUP_ID_CONFIG, groupId);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static Properties castPubProperties() {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, CastKeySerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, CastSerializer.class);
        return properties;
    }

    public static Properties targetedCastSubProperties(String groupId, int instanceId) {
        final Properties properties = new Properties();
        properties.put(APPLICATION_SERVER_CONFIG, applicationServer(instanceId));
        properties.put(APPLICATION_ID_CONFIG, groupId + instanceId);
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, CastKeyDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, CastDeserializer.class);
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, CastKeySerde.class);
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, CastSerde.class);
        properties.put(GROUP_ID_CONFIG, groupId);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static Properties targetedCastPubProperties() {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, CastKeySerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, CastSerializer.class);
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, CastKeySerde.class);
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, CastSerde.class);
        return properties;
    }

    public static Properties commandSubProperties(String commandSubscriber) {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, CommandDeserializer.class);
        properties.put(GROUP_ID_CONFIG, commandSubscriber);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static Properties commandPubProperties() {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, CommandSerializer.class);
        return properties;
    }

    public static Properties replySubProperties() {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(GROUP_ID_CONFIG, "theresonly-one-at-the-moment");
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static Properties replyPubProperties() {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }

    public static Properties activeSubProperties(String commandSubscriber) {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        properties.put(GROUP_ID_CONFIG, commandSubscriber);
        properties.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        return properties;
    }

    public static Properties activePubProperties() {
        final Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        return properties;
    }

    /** just to illustrate that partitions can be controlled explicitly. In this case we prioritize one originator
     * and then treat the remainders as equals */
    public static Properties initialCastStreamProperties() {
        final Properties properties = castPubProperties();
        properties.put(PARTITIONER_CLASS_CONFIG, CastPartitioner.class);
        return properties;
    }

    private KafkaProperties() {}
}
