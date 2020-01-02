package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

final class PartialAsyncCommitPolicy<K, V> extends AbstractPartialCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartialAsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private final OffsetCommitCallback callback;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    PartialAsyncCommitPolicy(Consumer<K, V> consumer, Duration forceWholeCommitInterval, int maxPendingAsyncCommits) {
        super(consumer, forceWholeCommitInterval);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        this.callback = new AsyncCommitCallback();
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        final Map<TopicPartition, OffsetAndMetadata> offsets = offsetsForPartialCommit();
        if (offsets.isEmpty()) {
            return Collections.emptySet();
        } else {
            final Set<TopicPartition> partitions = getCompletedPartitions(noPendingRecords);
            if (forceSync || pendingAsyncCommitCounter >= maxPendingAsyncCommits) {
                consumer.commitSync(offsets);
                pendingAsyncCommitCounter = 0;
                forceSync = false;
                clearCachedCompletedPartitionsRecords(partitions, noPendingRecords);
            } else {
                ++pendingAsyncCommitCounter;
                consumer.commitAsync(offsets, callback);
            }

            // update next recommit time even if async commit failed, we tolerate this situation
            updateNextRecommitTime();
            return partitions;
        }
    }

    int pendingAsyncCommitCount() {
        return pendingAsyncCommitCounter;
    }

    boolean forceSync() {
        return forceSync;
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
                final Map<TopicPartition, OffsetAndMetadata> completeOffsets =
                        offsets == completedTopicOffsets ? new HashMap<>(offsets) : offsets;
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : completeOffsets.entrySet()) {
                    completedTopicOffsets.remove(entry.getKey(), entry.getValue());
                    topicOffsetHighWaterMark.remove(entry.getKey(), entry.getValue().offset());
                }
            }
        }
    }
}
