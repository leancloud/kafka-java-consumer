package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

final class PartialSyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    PartialSyncCommitPolicy(Consumer<K, V> consumer, Duration forceWholeCommitInterval) {
        super(consumer, forceWholeCommitInterval);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        final Map<TopicPartition, OffsetAndMetadata> offsets = offsetsForPartialCommit();
        if (!offsets.isEmpty()) {
            consumer.commitSync(offsets);
        }

        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        } else {
            final Set<TopicPartition> partitions = getCompletedPartitions(noPendingRecords);
            clearCachedCompletedPartitionsRecords(partitions, noPendingRecords);
            return partitions;
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> offsetsForPartialCommit() {
        if (needRecommit()) {
            final Map<TopicPartition, OffsetAndMetadata> offsets = offsetsForRecommit();
            updateNextRecommitTime();
            return offsets;
        } else {
            return completedTopicOffsets;
        }
    }
}
