package com.crd.alpha.performance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import topics.PaymentOuterClass;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * TODO: Handle large files, where only part of the file is in the byte buffer
 *
 * Read first record - get size of event - add 6 to size and divide buffer size by
 * record size to get the number of records you can process per buffer
 */
public class FileExample {

    private final Properties props;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CachedSchemaRegistryClient schemaRegistry;

    public FileExample(Properties props, CachedSchemaRegistryClient schemaRegistry) {
        this.props = props;
        this.schemaRegistry = schemaRegistry;
    }

    /**
     * process with records obtained from Kafka Consumer
     */
    protected void writeFile() {
        byte[] serializedValue = null;

        final KafkaProtobufSerializer serializer = new KafkaProtobufSerializer(schemaRegistry);

        final String topic = props.getProperty("Topic");
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(new File("doc2.dat"));
            for (long i = 101; i <= 110; i++) {
                PaymentOuterClass.Payment payment =
                        PaymentOuterClass.Payment.newBuilder()
                                .setId(Long.toString(i))
                                .setAmount(1000.00d)
                                .build();
                serializedValue = serializer.serialize(topic, payment);
                if (serializedValue != null) {
                    System.out.print("Serialized value: ");
                    System.out.write(serializedValue);
                    System.out.println();
                }
                fos.write(serializedValue);
                fos.write("======".getBytes());

                System.out.println("Wrote data to the file");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) { }
        }
    }

    /**
     * Process creating Kafka events for Kafka Producer
     */
    protected void readFile() {
        ArrayList<byte[]> buffer = new ArrayList();
        int toIndex = -1;
        int fromIndex = 0;
        String topic = props.getProperty("Topic");
        /**
         * ReadFile seems to need the schema in the cached registry, rather than getting it on its own.
         * Have to resolve so readFile() can be called independently of writeFile()
         */
        final KafkaProtobufDeserializer deserializer = new KafkaProtobufDeserializer(schemaRegistry);

        try {
            Path path = Paths.get(props.getProperty("OutputFile"));
            // Perf test files will be too big to read this way
            byte[] binaryInput = Files.readAllBytes(path);
            while((toIndex = locateDelimiter(binaryInput, fromIndex)) > 0) {
                //System.out.println("Index: " + toIndex);
                byte[] messageInput = Arrays.copyOfRange(binaryInput, fromIndex, toIndex);
                buffer.add(messageInput);
                fromIndex = toIndex + 6;
            }
            for (int i = 0; i < buffer.size(); i++) {
                // have to know the schema in order to deserialize the topic
                Message message = deserializer.deserialize(topic, buffer.get(i));
                if(i == 0) {
                    ProtobufSchema schema = ProtobufSchemaUtils.getSchema(message);
                    System.out.println("\nSchema for message:\n" + schema.toString());
                }
                System.out.println("Deserialized Message read from file:\n " + message.toString());
            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch(IOException e) {
            e.printStackTrace();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    // return start of delimiter string "======"
    private static int locateDelimiter(byte[] array, int start) {
        boolean found_delimiter = false;
        int toIndex = -1;
        int fromIndex = start;
        int delimiterStart = -1;
        int length = -1;
        if(start >= array.length) {
            return -1;
        }
        for(int j = start; j < array.length; j++) {
            char testChar = (char) array[j];
            if(testChar == '=') {
                found_delimiter = true;
                delimiterStart = j;
                length = 1;
                toIndex = j;
                while(( ++j < array.length) && (testChar = (char) array[j]) == '=') {
                    length++;
                }
                if(length == 6) {
                    //System.out.println("Delimiter found: start " + delimiterStart + " end " + j);
                    return delimiterStart;
                }
            }
        }
        if(! found_delimiter) {
            return start;
        }
        return -1; // handles the delimiter after the last record in the file
    }
}
