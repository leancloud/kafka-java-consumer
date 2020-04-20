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
    public Set<TopicPartition> tryCommit(boolean noPendingRecords, ProcessRecordsProgress progress) {
        if (commitPaused() || progress.noOffsetsToCommit()) {
            return emptySet();
        }

        final Set<TopicPartition> partitions;
        if (noPendingRecords) {
            partitions = progress.allPartitions();
            progress.clearAll();
        } else {
            partitions = progress.completedPartitions();
            progress.clearFor(partitions);
        }

        return partitions;
    }
}
