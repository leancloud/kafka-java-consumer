package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

final class PartialAsyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartialAsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    PartialAsyncCommitPolicy(Consumer<K, V> consumer, int maxPendingAsyncCommits) {
        super(consumer);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<TopicPartition> partitions = getCompletedPartitions(noPendingRecords);
        if (forceSync || pendingAsyncCommitCounter >= maxPendingAsyncCommits) {
            consumer.commitSync(completedTopicOffsets);
            pendingAsyncCommitCounter = 0;
            forceSync = false;
            clearCachedCompletedPartitionsRecords(partitions, noPendingRecords);
        } else {
            ++pendingAsyncCommitCounter;
            consumer.commitAsync(completedTopicOffsets, (offsets, exception) -> {
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
            });
        }
        return partitions;
    }
}