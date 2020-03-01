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

import static cn.leancloud.kafka.consumer.ConsumerSeekDestination.NONE;
import static java.util.stream.Collectors.toSet;

final class RebalanceListener<K, V> implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    private final CommitPolicy<K, V> policy;
    private final Consumer<K, V> consumer;
    private final Set<TopicPartition> knownPartitions;
    private final ConsumerSeekDestination forceSeekTo;
    private Set<TopicPartition> pausedPartitions;

    RebalanceListener(Consumer<K, V> consumer, CommitPolicy<K, V> policy, ConsumerSeekDestination forceSeekTo) {
        this.policy = policy;
        this.consumer = consumer;
        this.pausedPartitions = Collections.emptySet();
        this.knownPartitions = new HashSet<>();
        this.forceSeekTo = forceSeekTo;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        pausedPartitions = consumer.paused();
        if (!pausedPartitions.isEmpty()) {
            pausedPartitions = new HashSet<>(pausedPartitions);
            pausedPartitions.removeAll(policy.syncPartialCommit());
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        if (forceSeekTo != NONE) {
            final Set<TopicPartition> newPartitions = new HashSet<>();
            for (TopicPartition p : partitions) {
                if (!knownPartitions.contains(p)) {
                    newPartitions.add(p);
                    logger.info("Assigned new partition: {}, force seeking it's offset to {}", p, forceSeekTo);
                }
            }

            if (!newPartitions.isEmpty()) {
                forceSeekTo.seek(consumer, newPartitions);
                knownPartitions.addAll(newPartitions);
            }
        }

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
