package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

final class PartialSyncCommitPolicy<K, V> extends AbstractPartialCommitPolicy<K, V> {
    PartialSyncCommitPolicy(Consumer<K, V> consumer, Duration forceWholeCommitInterval) {
        super(consumer, forceWholeCommitInterval);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        consumer.commitSync(offsetsToPartialCommit());

        final Set<TopicPartition> partitions = getCompletedPartitions(noPendingRecords);
        clearCachedCompletedPartitionsRecords(partitions, noPendingRecords);
        return partitions;
    }
}
