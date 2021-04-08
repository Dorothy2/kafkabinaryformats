package io.confluent.examples.clients.basicavro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.errors.SerializationException;
import topics.PaymentOuterClass;

import java.nio.ByteBuffer;
import java.util.Properties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileInputStream;
import java.io.InputStream;

public class SerializerDeserializer {

    private static final String TOPIC = "protobuf-transactions";
    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        if (args.length < 1) {
            // Backwards compatibility, assume localhost
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
            props.put("DEMO", "PROTO");
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

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        //if("AVRO".equals(props.get("DEMO"))) {
            final ObjectMapper objectMapper = new ObjectMapper();
            byte[] serializedValue = null;
            try {
                final CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("http://localhost:8081", 10);
                final KafkaProtobufSerializer serializer = new KafkaProtobufSerializer(schemaRegistry);
                final KafkaProtobufDeserializer deserializer = new KafkaProtobufDeserializer(schemaRegistry);

                for (long i = 101; i <= 102; i++) {
                    PaymentOuterClass.Payment payment =
                            PaymentOuterClass.Payment.newBuilder()
                                    .setId(Long.toString(i))
                                    .setAmount(1000.00d)
                                    .build();
                    serializedValue = serializer.serialize(TOPIC, payment);
                    if (serializedValue != null) {
                        System.out.print("Serialized value: ");
                        System.out.write(serializedValue);
                        System.out.println();
                    }

                    Message message = deserializer.deserialize(TOPIC, serializedValue);
                    System.out.println("Deserialized Message: " + message.toString());
                }
            } catch (final Exception e) {
                e.printStackTrace();
            }
        //} else {
           // System.out.println("Demo for Protobuf is not yet complete.");
        //}
    }
}

