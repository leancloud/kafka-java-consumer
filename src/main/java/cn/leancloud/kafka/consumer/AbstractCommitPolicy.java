package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Comparator.comparing;
import static java.util.function.BinaryOperator.maxBy;
import static java.util.stream.Collectors.toSet;

abstract class AbstractCommitPolicy<K, V> implements CommitPolicy<K, V> {
    protected final Consumer<K, V> consumer;
    final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    final Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets;

    AbstractCommitPolicy(Consumer<K, V> consumer) {
        this.consumer = consumer;
        this.topicOffsetHighWaterMark = new HashMap<>();
        this.completedTopicOffsets = new HashMap<>();
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record) {
        topicOffsetHighWaterMark.merge(
                new TopicPartition(record.topic(), record.partition()),
                record.offset() + 1,
                Math::max);
    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {
        completedTopicOffsets.merge(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1L),
                maxBy(comparing(OffsetAndMetadata::offset)));
    }

    @Override
    public Set<TopicPartition> partialCommit() {
        consumer.commitSync(completedTopicOffsets);
        final Set<TopicPartition> partitions = checkCompletedPartitions();
        completedTopicOffsets.clear();
        for (TopicPartition p : partitions) {
            topicOffsetHighWaterMark.remove(p);
        }
        return partitions;
    }

    Set<TopicPartition> getCompletedPartitions(boolean noPendingRecords) {
        final Set<TopicPartition> partitions;
        if (noPendingRecords) {
            assert checkCompletedPartitions().equals(topicOffsetHighWaterMark.keySet())
                    : "expect: " + checkCompletedPartitions() + " actual: " + topicOffsetHighWaterMark.keySet();
            partitions = new HashSet<>(topicOffsetHighWaterMark.keySet());
        } else {
            partitions = checkCompletedPartitions();
        }
        return partitions;
    }

    void clearCachedCompletedPartitionsRecords(Set<TopicPartition> completedPartitions, boolean noPendingRecords) {
        completedTopicOffsets.clear();
        if (noPendingRecords) {
            topicOffsetHighWaterMark.clear();
        } else {
            for (TopicPartition p : completedPartitions) {
                topicOffsetHighWaterMark.remove(p);
            }
        }
    }

    Map<TopicPartition, Long> topicOffsetHighWaterMark() {
        return topicOffsetHighWaterMark;
    }

    Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets() {
        return completedTopicOffsets;
    }

    private Set<TopicPartition> checkCompletedPartitions() {
        return completedTopicOffsets
                .entrySet()
                .stream()
                .filter(entry -> topicOffsetMeetHighWaterMark(entry.getKey(), entry.getValue()))
                .map(Map.Entry::getKey)
                .collect(toSet());
    }

    private boolean topicOffsetMeetHighWaterMark(TopicPartition topicPartition, OffsetAndMetadata offset) {
        final Long offsetHighWaterMark = topicOffsetHighWaterMark.get(topicPartition);
        if (offsetHighWaterMark != null) {
            return offset.offset() >= offsetHighWaterMark;
        }
        // maybe this partition revoked before a msg of this partition was processed
        return true;
    }
}
