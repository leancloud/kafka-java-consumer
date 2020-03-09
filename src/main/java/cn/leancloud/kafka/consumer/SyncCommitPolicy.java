package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Set;

final class SyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    SyncCommitPolicy(Consumer<K, V> consumer,
                     Duration syncCommitRetryInterval,
                     int maxAttemptsForEachSyncCommit,
                     Duration recommitInterval) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, recommitInterval);
    }

    @Override
    Set<TopicPartition> tryCommit0(boolean noPendingRecords) {
        if (!noPendingRecords || noTopicOffsetsToCommit()) {
            return Collections.emptySet();
        }

        final Set<TopicPartition> completePartitions = fullCommitSync();
        updateNextRecommitTime();
        return completePartitions;
    }
}
