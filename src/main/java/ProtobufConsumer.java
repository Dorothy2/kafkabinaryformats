import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.errors.SerializationException;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import topics.PaymentOuterClass;
import topics.PaymentOuterClass.Payment;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

/**
 * Reference article: https://dzone.com/articles/how-to-use-protobuf-with-apache-kafka-and-schema-r
 * for generic approach
 */
public class ProtobufConsumer {

    private static final String TOPIC = "protobuf-transactions";
    private static final Properties props = new Properties();
    private static String configFile;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(String[] args) throws IOException {

        if(args.length < 1) {
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payments-009");
        // set to true to allow the acknowledged offset to be bumped
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        props.put(KafkaProtobufDeserializerConfig.SPECIFIC_PROTOBUF_VALUE_TYPE, Payment.class.getName());
        //props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        //props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.RETRIES_CONFIG, 0);
        //props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer");
        props.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        //try (final KafkaConsumer<String, PaymentOuterClass.Payment> consumer = new KafkaConsumer<>(props)) {
        //    consumer.subscribe(Collections.singletonList(TOPIC));

            KafkaConsumer<String, Payment> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                ConsumerRecords<String, Payment> records = consumer.poll(Duration.ofMillis(100));
                //final ConsumerRecords<String, PaymentOuterClass.Payment> records = consumer.poll(Duration.ofMillis(100));
                for (final ConsumerRecord<String, Payment> record : records) {
                    System.out.printf("key = %s, value = %s%n", record.key(), record.value());
                    //PaymentOuterClass.Payment payment =
                    //        PaymentOuterClass.Payment.newBuilder()
                    //        .setId(record.value().getId())
                    //        .setAmount(record.value().getAmount())
                    //        .build();
                    //PaymentOuterClass.Payment payment = (PaymentOuterClass.Payment) record.value();
                    System.out.println("Payment id: " + record.value().getId() + " " + record.value().getAmount());
                }
            }

        }
    }
