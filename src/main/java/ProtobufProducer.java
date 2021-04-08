import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.SerializationException;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import topics.PaymentOuterClass;


import java.util.Properties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

public class ProtobufProducer {

    private static final String TOPIC = "protobuf-transactions";
    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) throws IOException {

        if(args == null || args.length == 0) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
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
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
        props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        try (KafkaProducer<String, PaymentOuterClass.Payment> producer = new KafkaProducer<String, PaymentOuterClass.Payment>(props)) {

            for (long i = 0; i < 10; i++) {
                final String orderId = "id" + Long.toString(i);
                PaymentOuterClass.Payment payment = PaymentOuterClass.Payment.newBuilder()
                        .setId(Long.toString(i))
                        .setAmount(1000.00d)
                        .build();
                final ProducerRecord<String, PaymentOuterClass.Payment> record = new ProducerRecord<String, PaymentOuterClass.Payment>(TOPIC, payment.getId().toString(), payment);
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Produce offset: " + metadata.offset());
                //Thread.sleep(1000L);
            }

            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        } catch (final ExecutionException e) {
            e.printStackTrace();
        }
    }

    //TODO Add a Protobuf consumer to make sure protobuf-transactions has the right # transactions and info


}
