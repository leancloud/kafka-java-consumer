package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Comparator.comparing;
import static java.util.function.BinaryOperator.maxBy;
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

    final Map<TopicPartition, Long> topicOffsetHighWaterMark;
    final Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets;
    protected final Consumer<K, V> consumer;
    private final long syncCommitRetryIntervalMs;
    private final int maxAttemptsForEachSyncCommit;

    AbstractCommitPolicy(Consumer<K, V> consumer, Duration syncCommitRetryInterval, int maxAttemptsForEachSyncCommit) {
        this.consumer = consumer;
        this.topicOffsetHighWaterMark = new HashMap<>();
        this.completedTopicOffsets = new HashMap<>();
        this.syncCommitRetryIntervalMs = syncCommitRetryInterval.toMillis();
        this.maxAttemptsForEachSyncCommit = maxAttemptsForEachSyncCommit;
    }

    @Override
    public void markPendingRecord(ConsumerRecord<K, V> record) {
        topicOffsetHighWaterMark.merge(
                new TopicPartition(record.topic(), record.partition()),
                record.offset() + 1,
                Math::max);
    }

    @Override
    public void markCompletedRecord(ConsumerRecord<K, V> record) {
        completedTopicOffsets.merge(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1L),
                maxBy(comparing(OffsetAndMetadata::offset)));
    }

    @Override
    public Set<TopicPartition> syncPartialCommit() {
        commitSync(completedTopicOffsets);
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

    @VisibleForTesting
    Map<TopicPartition, Long> topicOffsetHighWaterMark() {
        return topicOffsetHighWaterMark;
    }

    @VisibleForTesting
    Map<TopicPartition, OffsetAndMetadata> completedTopicOffsets() {
        return completedTopicOffsets;
    }

    void commitSync() {
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

    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
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

    private RetryContext context() {
        return new RetryContext(syncCommitRetryIntervalMs, maxAttemptsForEachSyncCommit);
    }
}
