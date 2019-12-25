package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class PartialSyncCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    PartialSyncCommitPolicy(Consumer<K, V> consumer) {
        super(consumer);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (completedTopicOffsets.isEmpty()) {
            return Collections.emptySet();
        }

        consumer.commitSync(completedTopicOffsets);

        final Set<TopicPartition> partitions;
        if (noPendingRecords) {
            assert checkCompletedPartitions().equals(topicOffsetHighWaterMark.keySet())
                    : "expect: " + checkCompletedPartitions() + " actual: " + topicOffsetHighWaterMark.keySet();
            partitions = new HashSet<>(topicOffsetHighWaterMark.keySet());
            completedTopicOffsets.clear();
            topicOffsetHighWaterMark.clear();
        } else {
            partitions = checkCompletedPartitions();
            completedTopicOffsets.clear();
            for (TopicPartition p : partitions) {
                topicOffsetHighWaterMark.remove(p);
            }
        }
        return partitions;
    }
}
