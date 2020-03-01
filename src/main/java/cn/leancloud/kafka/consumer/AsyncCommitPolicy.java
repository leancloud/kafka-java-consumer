package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class AsyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private final OffsetCommitCallback callback;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    AsyncCommitPolicy(Consumer<K, V> consumer,
                      Duration syncCommitRetryInterval,
                      int maxAttemptsForEachSyncCommit,
                      Duration recommitInterval,
                      int maxPendingAsyncCommits) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, recommitInterval);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        this.callback = new AsyncCommitCallback();
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (!noPendingRecords || completedTopicOffsets.isEmpty()) {
            if (needRecommit()) {
                commit(offsetsForRecommit());
            }
            return Collections.emptySet();
        }

        commit();

        final Set<TopicPartition> partitions = new HashSet<>(completedTopicOffsets.keySet());
        // it's OK to clear these collections here and we will not left any complete offset without commit even
        // when this async commit failed because if the async commit failed we will do a sync commit after all
        completedTopicOffsets.clear();
        topicOffsetHighWaterMark.clear();
        return partitions;
    }

    @VisibleForTesting
    int pendingAsyncCommitCount() {
        return pendingAsyncCommitCounter;
    }

    @VisibleForTesting
    boolean forceSync() {
        return forceSync;
    }

    @VisibleForTesting
    void setForceSync(boolean forceSync) {
        this.forceSync = forceSync;
    }

    private void commit() {
        commit(Collections.emptyMap());
    }

    private void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (forceSync || pendingAsyncCommitCounter >= maxPendingAsyncCommits) {
            syncCommit(offsets);
            pendingAsyncCommitCounter = 0;
            forceSync = false;
        } else {
            asyncCommit(offsets);
        }

        // for our commit policy, no matter syncCommit or asyncCommit we use, we always
        // commit all assigned offsets, so we can update recommit time here safely. And
        // we don't mind that if the async commit request failed, we tolerate this situation
        updateNextRecommitTime();
    }

    private void asyncCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        ++pendingAsyncCommitCounter;
        if (offsets.isEmpty()) {
            consumer.commitAsync(callback);
        } else {
            consumer.commitAsync(offsets, callback);
        }
    }

    private void syncCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty()) {
            commitSync();
        } else {
            commitSync(offsets);
        }
    }

    private class AsyncCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            --pendingAsyncCommitCounter;
            assert pendingAsyncCommitCounter >= 0 : "actual: " + pendingAsyncCommitCounter;
            if (exception != null) {
                logger.warn("Failed to commit offsets: " + offsets + " asynchronously", exception);
                forceSync = true;
            }
        }
    }
}
