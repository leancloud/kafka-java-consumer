package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class AsyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    AsyncCommitPolicy(Consumer<K, V> consumer, int maxPendingAsyncCommits) {
        super(consumer);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (!noPendingRecords || completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        if (forceSync || pendingAsyncCommitCounter >= maxPendingAsyncCommits) {
            consumer.commitSync();
            pendingAsyncCommitCounter = 0;
            forceSync = false;
        } else {
            ++pendingAsyncCommitCounter;
            consumer.commitAsync((offsets, exception) -> {
                --pendingAsyncCommitCounter;
                assert pendingAsyncCommitCounter >= 0 : "actual: " + pendingAsyncCommitCounter;
                if (exception != null) {
                    logger.warn("Failed to commit offsets: " + offsets + " asynchronously", exception);
                    forceSync = true;

                }
            });
        }

        final Set<TopicPartition> partitions = new HashSet<>(completedTopicOffsets.keySet());
        completedTopicOffsets.clear();
        topicOffsetHighWaterMark.clear();
        return partitions;
    }
}
