package com.crd.alpha.consumer.metrics;

import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class ConsumerOffsets {
    private static final String TOPIC = "protobuf-transactions";
    private static final String GROUP_ID = "test-payments-009";
    private static final String HOST = "localhost:9092";
    private static final Properties props = new Properties();
    private static String configFile;

    public static void main(String... vj) {
        ConsumerOffsets cgl = new ConsumerOffsets();
        Map<TopicPartition, Long> originalOffsets = new HashMap();

        while (true) {

            Map<TopicPartition, PartitionOffsets> lag = cgl.getConsumerGroupOffsets(HOST, TOPIC, GROUP_ID);
            //System.out.println("$$LAG = " + lag);
            System.out.println(lag);
            boolean ended = false;
            for(TopicPartition key : lag.keySet()) {
                PartitionOffsets offsets = lag.get(key);
                if(offsets.getNoLagTimestamp() != -1) {
                    ended = true;
                    break;
                }
            }
            if(ended) {
                System.exit(1);
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Map<TopicPartition, PartitionOffsets> getConsumerGroupOffsets(String host, String topic, String groupId) {
        Map<TopicPartition, Long> logEndOffset = getLogEndOffset(topic, host);

        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (Map.Entry<TopicPartition, Long> s : logEndOffset.entrySet()) {
            topicPartitions.add(s.getKey());
        }

        KafkaConsumer<String, Object> consumer = createNewConsumer(groupId, host);
        Map<TopicPartition, OffsetAndMetadata> comittedOffsetMeta = consumer.committed(topicPartitions);

        BinaryOperator<PartitionOffsets> mergeFunction = (a, b) -> {
            throw new IllegalStateException();
        };
        Map<TopicPartition, PartitionOffsets> result = logEndOffset.entrySet().stream()
                .collect(Collectors.toMap(entry -> (entry.getKey()), entry -> {
                    OffsetAndMetadata committed = comittedOffsetMeta.get(entry.getKey());
                    long currentOffset = 0;
                    if(committed != null) { //committed offset will be null for unknown consumer groups
                        currentOffset = committed.offset();
                    }
                    return new PartitionOffsets(entry.getValue(), currentOffset, entry.getKey().partition(), topic);
                }, mergeFunction));

        return result;
    }

    private final String monitoringConsumerGroupID = "monitoring_consumer_" + UUID.randomUUID().toString();

    private static KafkaConsumer<String, Object> createNewConsumer(String groupId, String host) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // Here auto-commit can be false. You are looking at the consumer's metadata, not consuming records
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaProtobufDeserializer.class);
        properties.put(KafkaProtobufDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        return new KafkaConsumer<>(properties);
    }

    public Map<TopicPartition, Long> getLogEndOffset(String topic, String host) {
        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
        KafkaConsumer<?, ?> consumer = createNewConsumer(monitoringConsumerGroupID, host);
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = partitionInfoList.stream()
                .map(pi -> new TopicPartition(topic, pi.partition())).collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        topicPartitions.forEach(topicPartition -> endOffsets.put(topicPartition, consumer.position(topicPartition)));
        consumer.close();
        return endOffsets;
    }

    private static class PartitionOffsets {
        private long lag;
        private long timestamp = System.currentTimeMillis();
        private long noLagTimestamp = -1;
        private long endOffset;
        private long currentOffset;
        private int partition;
        private String topic;

        SimpleDateFormat sdf = new SimpleDateFormat("MMM dd,yyyy HH:mm:ss.SSS");

        public PartitionOffsets(long endOffset, long currentOffset, int partition, String topic) {
            this.endOffset = endOffset;
            this.currentOffset = currentOffset;
            this.partition = partition;
            this.topic = topic;
            this.lag = endOffset - currentOffset;
            this.noLagTimestamp = lag == 0 ? System.currentTimeMillis() : -1;
        }

        public long getNoLagTimestamp() {
            return noLagTimestamp;
        }

        @Override
        public String toString() {
            //return sdf.format(timestamp) + " PartitionOffsets [lag=" + lag + ", endOffset=" + endOffset
            //        + ", currentOffset=" + currentOffset + ", partition=" + partition + ", topic=" + topic + "]";
            if(noLagTimestamp !=  -1) {
                return sdf.format(timestamp) + " PartitionOffsets [lag=" + lag + ", endOffset=" + endOffset
                        + ", currentOffset=" + currentOffset + ", lagEnded=" + sdf.format(noLagTimestamp) + "]";
            } else {
                return sdf.format(timestamp) + " PartitionOffsets [lag=" + lag + ", endOffset=" + endOffset
                        + ", currentOffset=" + currentOffset + "]";
            }
        }

    }
}
