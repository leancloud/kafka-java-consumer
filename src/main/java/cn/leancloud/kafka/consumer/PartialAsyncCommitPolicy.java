package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

final class PartialAsyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartialAsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private final OffsetCommitCallback callback;
    private final Map<TopicPartition, OffsetAndMetadata> pendingAsyncCommitOffset;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    PartialAsyncCommitPolicy(Consumer<K, V> consumer,
                             Duration syncCommitRetryInterval,
                             int maxAttemptsForEachSyncCommit,
                             Duration forceWholeCommitInterval,
                             int maxPendingAsyncCommits) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, forceWholeCommitInterval);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        this.callback = new AsyncCommitCallback();
        this.pendingAsyncCommitOffset = new HashMap<>();
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        final boolean syncCommit = forceSync || pendingAsyncCommitCounter >= maxPendingAsyncCommits;
        final Map<TopicPartition, OffsetAndMetadata> offsets = offsetsForPartialCommit(syncCommit);

        if (offsets.isEmpty()) {
            return Collections.emptySet();
        } else {
            final Set<TopicPartition> partitions = getCompletedPartitions(noPendingRecords);
            if (syncCommit) {
                commitSync(offsets);
                pendingAsyncCommitOffset.clear();
                pendingAsyncCommitCounter = 0;
                forceSync = false;
                clearCachedCompletedPartitionsRecords(partitions, noPendingRecords);
            } else {
                ++pendingAsyncCommitCounter;
                consumer.commitAsync(offsets, callback);
                pendingAsyncCommitOffset.putAll(offsets);
            }
            return partitions;
        }
    }

    @VisibleForTesting
    int pendingAsyncCommitCount() {
        return pendingAsyncCommitCounter;
    }

    @VisibleForTesting
    boolean forceSync() {
        return forceSync;
    }

    private Map<TopicPartition, OffsetAndMetadata> offsetsForPartialCommit(boolean syncCommit) {
        final Map<TopicPartition, OffsetAndMetadata> offsets;
        if (needRecommit()) {
            offsets = offsetsForRecommit();
            // we tolerate the commit failure when using async commit
            updateNextRecommitTime();
        } else if (syncCommit) {
            offsets = completedTopicOffsets;
        } else {
            offsets = new HashMap<>(completedTopicOffsets);
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : pendingAsyncCommitOffset.entrySet()) {
                offsets.remove(entry.getKey(), entry.getValue());
            }
        }
        return offsets;
    }

    private class AsyncCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            --pendingAsyncCommitCounter;
            assert pendingAsyncCommitCounter >= 0 : "actual: " + pendingAsyncCommitCounter;
            if (exception != null) {
                // if last async commit is failed, we do not clean cached completed offsets and let next
                // commit be a sync commit so all the complete offsets will be committed at that time
                logger.warn("Failed to commit offset: " + offsets + " asynchronously", exception);
                forceSync = true;
            } else {
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                    topicOffsetHighWaterMark.remove(entry.getKey(), entry.getValue().offset());
                    // we don't clear pendingAsyncCommitOffset here
                    // because they are cleared on sync commit which occurs every maxPendingAsyncCommits async commits
                    // todo: maybe we don't need to clean completedTopicOffsets too
                    completedTopicOffsets.remove(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
