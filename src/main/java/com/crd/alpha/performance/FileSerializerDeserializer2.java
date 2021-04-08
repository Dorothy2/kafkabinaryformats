package com.crd.alpha.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import topics.PaymentOuterClass;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class FileSerializerDeserializer2 {

    private static final String TOPIC = "protobuf-transactions";
    private static final String OUTPUT_FILE = "doc2.dat";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        if (args.length < 1) {
            // Backwards compatibility, assume localhost
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        } else {
            // Load properties from a local configuration file
            // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
            // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
            // Documentation at https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
            configFile = args[0];
            if (!Files.exists(Paths.get(configFile))) {
                throw new IOException(configFile + " not found.");
            } else {
                try (InputStream inputStream = new FileInputStream(configFile)) {
                    props.load(inputStream);
                }
            }
        }

        props.put("OutputFile", OUTPUT_FILE);
        props.put("SchemaRegistryUrl", SCHEMA_REGISTRY_URL);
        props.put("Topic", TOPIC);
        final CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("http://localhost:8081", 10);

        FileExample example = new FileExample(props, schemaRegistry);

        example.writeFile();

        example.readFile();

  }
}

