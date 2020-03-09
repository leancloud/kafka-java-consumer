package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;

class ProcessRecordsProgress {
    private final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    private final Map<TopicPartition, CompletedOffsets> completedOffsets;

    public ProcessRecordsProgress() {
        this.topicOffsetHighWaterMark = new HashMap<>();
        this.completedOffsets = new HashMap<>();
    }

    /**
     * Mark an {@link ConsumerRecord} as pending before processing it. So {@link CommitPolicy} can know which and
     * how many records we need to process. It is called by {@link Fetcher} when {@code Fetcher} fetched any
     * {@link ConsumerRecord}s from Broker.
     *
     * @param record the {@link ConsumerRecord} need to process
     */
    void markPendingRecord(ConsumerRecord<?, ?> record) {
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        topicOffsetHighWaterMark.merge(
                topicPartition,
                record.offset() + 1,
                Math::max);

        final CompletedOffsets offset = completedOffsets.get(topicPartition);
        // offset could be null when there're duplicate records consumed from broker
        if (offset == null) {
            completedOffsets.put(topicPartition, new CompletedOffsets(record.offset() - 1L));
        }
    }

    /**
     * Mark an {@link ConsumerRecord} as completed after processing it. So {@link CommitPolicy} can know which and
     * how many records we have processed. It is called by {@link Fetcher} when {@code Fetcher} make sure that
     * a {@code ConsumerRecord} was processed successfully.
     *
     * @param record the {@link ConsumerRecord} processed
     */
    void markCompletedRecord(ConsumerRecord<?, ?> record) {
        final CompletedOffsets offset = completedOffsets.get(new TopicPartition(record.topic(), record.partition()));
        // offset could be null, when the partition of the record was revoked before its processing was done
        if (offset != null) {
            offset.addCompleteOffset(record.offset());
        }
    }

    void clearAll() {
        topicOffsetHighWaterMark.clear();
        completedOffsets.clear();
    }

    void clearFor(Collection<TopicPartition> partitions) {
        for (TopicPartition p : partitions) {
            topicOffsetHighWaterMark.remove(p);
            completedOffsets.remove(p);
        }
    }

    @VisibleForTesting
    Map<TopicPartition, Long> pendingRecordOffsets() {
        return topicOffsetHighWaterMark;
    }

    boolean noPendingRecords() {
        return topicOffsetHighWaterMark.isEmpty();
    }

    Set<TopicPartition> allPartitions() {
        return new HashSet<>(topicOffsetHighWaterMark.keySet());
    }

    boolean noCompletedRecords() {
        return completedOffsets.isEmpty();
    }

    Map<TopicPartition, OffsetAndMetadata> completedOffsetsToCommit() {
        if (noCompletedRecords()) {
            return emptyMap();
        }

        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (Map.Entry<TopicPartition, CompletedOffsets> entry : completedOffsets.entrySet()) {
            final CompletedOffsets offset = entry.getValue();
            if (offset.hasOffsetToCommit()) {
                offsets.put(entry.getKey(), offset.getOffsetToCommit());
            }
        }

        return offsets;
    }

    boolean noOffsetsToCommit() {
        if (noCompletedRecords()) {
            return true;
        }

        for (Map.Entry<TopicPartition, CompletedOffsets> entry : completedOffsets.entrySet()) {
            final CompletedOffsets offset = entry.getValue();
            if (offset.hasOffsetToCommit()) {
                return false;
            }
        }

        return true;
    }

    void updateCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final CompletedOffsets offset = completedOffsets.get(entry.getKey());
            offset.updateCommittedOffset(entry.getValue().offset());
        }
    }

    Set<TopicPartition> clearCompletedPartitions(Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        final Set<TopicPartition> partitions = completedPartitions(committedOffsets);
        clearFor(partitions);
        return partitions;
    }

    Set<TopicPartition> completedPartitions() {
        return completedPartitions(completedOffsetsToCommit());
    }

    Set<TopicPartition> completedPartitions(Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        return committedOffsets
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
