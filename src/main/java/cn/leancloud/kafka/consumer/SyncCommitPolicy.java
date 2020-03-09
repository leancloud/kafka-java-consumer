package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Set;

import static java.util.Collections.emptySet;

final class SyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    SyncCommitPolicy(Consumer<K, V> consumer,
                     Duration syncCommitRetryInterval,
                     int maxAttemptsForEachSyncCommit,
                     Duration recommitInterval) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, recommitInterval);
    }

    @Override
    Set<TopicPartition> tryCommit0(boolean noPendingRecords, ProcessRecordsProgress progress) {
        if (!noPendingRecords || progress.noOffsetsToCommit()) {
            return emptySet();
        }

        final Set<TopicPartition> completePartitions = fullCommitSync(progress);
        updateNextRecommitTime();
        return completePartitions;
    }
}
