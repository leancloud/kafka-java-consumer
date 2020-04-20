package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;

import java.time.Duration;
import java.util.*;

import static java.util.Collections.emptySet;

abstract class AbstractCommitPolicy<K, V> implements CommitPolicy {
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
    private final long syncCommitRetryIntervalMs;
    private final int maxAttemptsForEachSyncCommit;
    private boolean commitPuased;

    AbstractCommitPolicy(Consumer<K, V> consumer, Duration syncCommitRetryInterval, int maxAttemptsForEachSyncCommit) {
        this.consumer = consumer;
        this.syncCommitRetryIntervalMs = syncCommitRetryInterval.toMillis();
        this.maxAttemptsForEachSyncCommit = maxAttemptsForEachSyncCommit;
    }

    @Override
    public Set<TopicPartition> partialCommitSync(ProcessRecordsProgress progress) {
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = progress.completedOffsetsToCommit();
        if (offsetsToCommit.isEmpty()) {
            return emptySet();
        }
        commitSyncWithRetry(offsetsToCommit);
        progress.updateCommittedOffsets(offsetsToCommit);

        return progress.clearCompletedPartitions(offsetsToCommit);
    }

    @Override
    public void pauseCommit() {
        commitPuased = true;
    }

    @Override
    public void resumeCommit() {
        commitPuased = false;
    }

    @Override
    public boolean commitPaused() {
        return commitPuased;
    }

    Set<TopicPartition> fullCommitSync(ProcessRecordsProgress progress) {
        commitSyncWithRetry();

        final Set<TopicPartition> completePartitions = progress.allPartitions();
        progress.clearAll();
        return completePartitions;
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

    private RetryContext context() {
        return new RetryContext(syncCommitRetryIntervalMs, maxAttemptsForEachSyncCommit);
    }
}
