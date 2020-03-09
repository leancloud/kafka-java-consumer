package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.function.Function;
import java.util.stream.LongStream;

import static java.util.Comparator.comparing;
import static java.util.function.BinaryOperator.maxBy;
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

    static Map<TopicPartition, OffsetAndMetadata> buildCommitOffsets(List<ConsumerRecord<Object, Object>> records) {
        final Map<TopicPartition, OffsetAndMetadata> completeOffsets = new HashMap<>();
        for (ConsumerRecord<Object, Object> record : records) {
            completeOffsets.merge(new TopicPartition(testingTopic, record.partition()),
                    new OffsetAndMetadata(record.offset() + 1),
                    maxBy(comparing(OffsetAndMetadata::offset)));
        }
        return completeOffsets;
    }

    static <K, V> void fireConsumerRecords(MockConsumer<K, V> consumer, Collection<ConsumerRecord<K, V>> records) {
        for (ConsumerRecord<K, V> record : records) {
            consumer.addRecord(record);
        }
    }

    static List<ConsumerRecord<Object, Object>> generateConsumedRecords(MockConsumer<Object, Object> consumer, List<TopicPartition> partitions) {
        return generateConsumedRecords(consumer, partitions, 1);
    }

    static List<ConsumerRecord<Object, Object>> generateConsumedRecords(MockConsumer<Object, Object> consumer, List<TopicPartition> partitions, int size) {
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, size);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        return pendingRecords;
    }

    static List<ConsumerRecord<Object, Object>> addPendingRecordsInPolicy(ProcessRecordsProgress progress, List<ConsumerRecord<Object, Object>> records) {
        for (ConsumerRecord<Object, Object> record : records) {
            addPendingRecordInPolicy(progress, record);
        }
        return records;
    }

    static void addPendingRecordInPolicy(ProcessRecordsProgress progress, ConsumerRecord<Object, Object> record) {
        progress.markPendingRecord(record);
    }

    static List<ConsumerRecord<Object, Object>> addCompleteRecordsInPolicy(ProcessRecordsProgress progress, List<ConsumerRecord<Object, Object>> records) {
        for (ConsumerRecord<Object, Object> record : records) {
            addCompleteRecordInPolicy(progress, record);
        }
        return records;
    }

    static void addCompleteRecordInPolicy(ProcessRecordsProgress progress, ConsumerRecord<Object, Object> record) {
        progress.markPendingRecord(record);
        progress.markCompletedRecord(record);
    }
}
