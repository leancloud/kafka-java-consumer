package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class SyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    SyncCommitPolicy(Consumer<K, V> consumer) {
        super(consumer);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (noPendingRecords && !completedTopicOffsets.isEmpty()) {
            consumer.commitSync();
            final Set<TopicPartition> completePartitions = new HashSet<>(completedTopicOffsets.keySet());
            completedTopicOffsets.clear();
            topicOffsetHighWaterMark.clear();
            return completePartitions;
        }
        return Collections.emptySet();
    }
}
