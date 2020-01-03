package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

abstract class AbstractRecommitAwareCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private final Duration recommitInterval;
    private long nextRecommitNanos;

    AbstractRecommitAwareCommitPolicy(Consumer<K, V> consumer, Duration recommitInterval) {
        super(consumer);
        this.recommitInterval = recommitInterval;
        updateNextRecommitTime(System.nanoTime());
    }

    Map<TopicPartition, OffsetAndMetadata> offsetsForRecommit() {
        assert needRecommit() : "current nanos: " + System.nanoTime() + " nextRecommitNanos:" + nextRecommitNanos;

        final Map<TopicPartition, OffsetAndMetadata> ret = new HashMap<>(completedTopicOffsets);
        for (TopicPartition partition : consumer.assignment()) {
            final OffsetAndMetadata offset = consumer.committed(partition);
            if (offset != null) {
                ret.putIfAbsent(partition, offset);
            }
        }

        return ret;
    }

    boolean needRecommit() {
        return System.nanoTime() >= nextRecommitNanos;
    }

    void updateNextRecommitTime() {
        updateNextRecommitTime(System.nanoTime());
    }

    @VisibleForTesting
    long nextRecommitNanos() {
        return nextRecommitNanos;
    }

    private void updateNextRecommitTime(long currentNanos) {
        nextRecommitNanos = currentNanos + recommitInterval.toNanos();
    }
}
