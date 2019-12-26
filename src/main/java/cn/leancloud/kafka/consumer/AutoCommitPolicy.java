package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Set;

class AutoCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    AutoCommitPolicy(Consumer<K, V> consumer) {
        super(consumer);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        final Set<TopicPartition> partitions = getCompletedPartitions(noPendingRecords);
        clearCachedCompletedPartitionsRecords(partitions, noPendingRecords);
        return partitions;
    }
}
