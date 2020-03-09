package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

import java.time.Duration;
import java.util.*;

import static java.util.stream.Collectors.toSet;

abstract class AbstractCommitPolicy<K, V> implements CommitPolicy<K, V> {
    static SleepFunction sleepFunction = Thread::sleep;

    interface SleepFunction {
        void sleep(long timeout) throws InterruptedException;
    }

    private static class RetryContext {
        private final long retryInterval;
        private final int maxAttempts;
        private int numOfAttempts;

        private RetryContext(long retryInterval, int maxAttempts) {
            this.retryInterval = retryInterval;
            this.maxAttempts = maxAttempts;
            this.numOfAttempts = 0;
        }

        void onError(RetriableException e) {
            if (++numOfAttempts >= maxAttempts) {
                throw e;
            } else {
                try {
                    sleepFunction.sleep(retryInterval);
                } catch (InterruptedException ex) {
                    e.addSuppressed(ex);
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    protected final Consumer<K, V> consumer;
    private final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    private final Map<TopicPartition, CompletedOffsets> completedOffsets;
    private final long syncCommitRetryIntervalMs;
    private final int maxAttemptsForEachSyncCommit;

    AbstractCommitPolicy(Consumer<K, V> consumer, Duration syncCommitRetryInterval, int maxAttemptsForEachSyncCommit) {
        this.consumer = consumer;
        this.topicOffsetHighWaterMark = new HashMap<>();
        this.completedOffsets = new HashMap<>();
        this.syncCommitRetryIntervalMs = syncCommitRetryInterval.toMillis();
        this.maxAttemptsForEachSyncCommit = maxAttemptsForEachSyncCommit;
    }

    @Override
    public void markPendingRecord(ConsumerRecord<K, V> record) {
        final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
        topicOffsetHighWaterMark.merge(
                topicPartition,
                record.offset() + 1,
                Math::max);

        final CompletedOffsets offset = completedOffsets.get(topicPartition);
        // please note that if offset exists, it could happen for record.offset() >= offset.nextOffsetToCommit()
        // when there're duplicate records which have lower offset than our next offset to commit consumed from broker
        if (offset == null) {
            completedOffsets.put(topicPartition, new CompletedOffsets(record.offset() - 1L));
        }
    }

    @Override
    public void markCompletedRecord(ConsumerRecord<K, V> record) {
        final CompletedOffsets offset = completedOffsets.get(new TopicPartition(record.topic(), record.partition()));
        // offset could be null, when the partition of the record was revoked before its processing was done
        if (offset != null) {
            offset.addCompleteOffset(record.offset());
        }
    }

    @Override
    public void revokePartitions(Collection<TopicPartition> partitions) {
        clearProcessingRecordStatesFor(partitions);
    }

    @Override
    public Set<TopicPartition> partialCommitSync() {
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = completedTopicOffsetsToCommit();
        if (offsetsToCommit.isEmpty()) {
            return Collections.emptySet();
        }
        commitSyncWithRetry(offsetsToCommit);
        updatePartialCommittedOffsets(offsetsToCommit);

        return clearProcessingRecordStatesForCompletedPartitions(offsetsToCommit);
    }

    Set<TopicPartition> fullCommitSync() {
        commitSyncWithRetry();

        final Set<TopicPartition> completePartitions = partitionsForAllRecordsStates();
        clearAllProcessingRecordStates();
        return completePartitions;
    }

    @VisibleForTesting
    Map<TopicPartition, Long> topicOffsetHighWaterMark() {
        return topicOffsetHighWaterMark;
    }

    Map<TopicPartition, OffsetAndMetadata> completedTopicOffsetsToCommit() {
        if (noCompletedOffsets()) {
            return Collections.emptyMap();
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

    boolean noTopicOffsetsToCommit() {
        if (noCompletedOffsets()) {
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

    void updatePartialCommittedOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            final CompletedOffsets offset = completedOffsets.get(entry.getKey());
            offset.updateCommittedOffset(entry.getValue().offset());
        }
    }

    boolean noCompletedOffsets() {
        return completedOffsets.isEmpty();
    }

    void commitSyncWithRetry() {
        final RetryContext context = context();
        do {
            try {
                consumer.commitSync();
                return;
            } catch (RetriableException e) {
                context.onError(e);
            }
        } while (true);
    }

    void commitSyncWithRetry(Map<TopicPartition, OffsetAndMetadata> offsets) {
        final RetryContext context = context();
        do {
            try {
                consumer.commitSync(offsets);
                return;
            } catch (RetriableException e) {
                context.onError(e);
            }
        } while (true);
    }

    Set<TopicPartition> partitionsForAllRecordsStates() {
        return new HashSet<>(topicOffsetHighWaterMark.keySet());
    }

    void clearAllProcessingRecordStates() {
        topicOffsetHighWaterMark.clear();
        completedOffsets.clear();
    }

    Set<TopicPartition> clearProcessingRecordStatesForCompletedPartitions(Map<TopicPartition, OffsetAndMetadata> committedOffsets) {
        final Set<TopicPartition> partitions = partitionsToSafeResume(committedOffsets);
        clearProcessingRecordStatesFor(partitions);
        return partitions;
    }

    void clearProcessingRecordStatesFor(Collection<TopicPartition> partitions) {
        for (TopicPartition p : partitions) {
            topicOffsetHighWaterMark.remove(p);
            completedOffsets.remove(p);
        }
    }

    Set<TopicPartition> partitionsToSafeResume() {
        return partitionsToSafeResume(completedTopicOffsetsToCommit());
    }

    Set<TopicPartition> partitionsToSafeResume(Map<TopicPartition, OffsetAndMetadata> completedOffsets) {
        return completedOffsets
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

    private RetryContext context() {
        return new RetryContext(syncCommitRetryIntervalMs, maxAttemptsForEachSyncCommit);
    }
}
