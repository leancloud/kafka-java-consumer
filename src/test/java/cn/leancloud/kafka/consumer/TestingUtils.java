package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.LongStream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

class TestingUtils {
    static final String testingTopic = "TestingTopic";
    static final Object defaultKey = new Object();
    static final Object defaultMsg = new Object();

    static List<TopicPartition> toPartitions(List<Integer> partitions) {
        return partitions
                .stream()
                .map(p -> new TopicPartition(testingTopic, p))
                .collect(toList());
    }

    static void assignPartitions(MockConsumer<?, ?> consumer, Collection<TopicPartition> partitions, long offsets) {
        final Map<TopicPartition, Long> partitionOffset = partitions
                .stream()
                .collect(toMap(Function.identity(), (p) -> offsets));

        consumer.addEndOffsets(partitionOffset);
        consumer.assign(partitionOffset.keySet());
    }

    static List<ConsumerRecord<Object, Object>> prepareConsumerRecords(Collection<TopicPartition> partitions,
                                                                       long offsetStart,
                                                                       int size) {
        final List<ConsumerRecord<Object, Object>> records = new ArrayList<>();

        for (TopicPartition partition : partitions) {
            records.addAll(LongStream.range(offsetStart, offsetStart + size)
                    .boxed()
                    .map(offset -> new ConsumerRecord<>(
                            testingTopic,
                            partition.partition(),
                            offset,
                            defaultKey,
                            defaultMsg))
                    .collect(toList()));

        }

        return records;
    }

    static <K, V> void fireConsumerRecords(MockConsumer<K, V> consumer, Collection<ConsumerRecord<K, V>> records) {
        for (ConsumerRecord<K, V> record : records) {
            consumer.addRecord(record);
        }
    }
}
