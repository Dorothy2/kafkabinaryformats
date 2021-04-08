import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import topics.PaymentOuterClass;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class FileSerializerDeserializer {

    private static final String TOPIC = "protobuf-transactions";
    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        if (args.length < 1) {
            // Backwards compatibility, assume localhost
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
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
        Path path = Paths.get("doc.txt");
        final ObjectMapper objectMapper = new ObjectMapper();
        byte[] serializedValue = null;
        try {
            final CachedSchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient("http://localhost:8081", 10);
            final KafkaProtobufSerializer serializer = new KafkaProtobufSerializer(schemaRegistry);
            final KafkaProtobufDeserializer deserializer = new KafkaProtobufDeserializer(schemaRegistry);

            for (long i = 101; i <= 101; i++) {
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

                try {
                    Files.write(path, serializedValue);
                    System.out.println("Successfully written data to the file");
                } catch(IOException e) {
                    e.printStackTrace();
                }

                String content = null;
                try {
                    byte[] binaryInput = Files.readAllBytes(path);
                    Message message = deserializer.deserialize(TOPIC, binaryInput);
                    System.out.println("Deserialized Message read from file: " + message.toString());
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}

