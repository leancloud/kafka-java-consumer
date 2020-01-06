package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

final class RebalanceListener<K, V> implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    private final CommitPolicy<K, V> policy;
    private final Consumer<K, V> consumer;
    private Set<TopicPartition> pausedPartitions;

    RebalanceListener(Consumer<K, V> consumer, CommitPolicy<K, V> policy) {
        this.policy = policy;
        this.consumer = consumer;
        this.pausedPartitions = Collections.emptySet();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        pausedPartitions = consumer.paused();
        if (!pausedPartitions.isEmpty()) {
            pausedPartitions = new HashSet<>(pausedPartitions);
            pausedPartitions.removeAll(policy.partialCommit());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (!pausedPartitions.isEmpty()) {
            final Set<TopicPartition> partitionToPause = partitions
                    .stream()
                    .filter(p -> pausedPartitions.contains(p))
                    .collect(toSet());
            if (partitionToPause.isEmpty()) {
                logger.info("Previous paused partitions: {} were all revoked", pausedPartitions);
            } else {
                logger.info("Pause previous paused partitions: {}", partitionToPause);
                consumer.pause(partitionToPause);
            }

            pausedPartitions = Collections.emptySet();
        }
    }
}
