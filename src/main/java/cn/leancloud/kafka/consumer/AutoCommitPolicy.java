package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Set;

import static java.util.Collections.emptySet;

final class AutoCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    AutoCommitPolicy(Consumer<K, V> consumer) {
        // For auto commit policy we don't commit by hand so we don't need retry sync commit facilities
        super(consumer, Duration.ZERO, 1);
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        if (noTopicOffsetsToCommit()) {
            return emptySet();
        }

        final Set<TopicPartition> partitions;
        if (noPendingRecords) {
            partitions = partitionsForAllRecordsStates();
            clearAllProcessingRecordStates();
        } else {
            partitions = partitionsToSafeResume();
            clearProcessingRecordStatesFor(partitions);
        }

        return partitions;
    }
}
