package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class SyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    SyncCommitPolicy(Consumer<K, V> consumer,
                     Duration syncCommitRetryInterval,
                     int maxAttemptsForEachSyncCommit,
                     Duration recommitInterval) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, recommitInterval);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (noPendingRecords && !completedTopicOffsets.isEmpty()) {
            commitSync();
            final Set<TopicPartition> completePartitions = new HashSet<>(completedTopicOffsets.keySet());
            completedTopicOffsets.clear();
            topicOffsetHighWaterMark.clear();
            updateNextRecommitTime();
            return completePartitions;
        } else if (needRecommit()) {
            commitSync(offsetsForRecommit());
            updateNextRecommitTime();
        }
        return Collections.emptySet();
    }
}
