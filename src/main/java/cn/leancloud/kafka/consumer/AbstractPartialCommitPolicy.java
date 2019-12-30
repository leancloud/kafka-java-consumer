package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

abstract class AbstractPartialCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private final Duration forceWholeCommitInterval;
    private long nextWholeCommitNanos;

    AbstractPartialCommitPolicy(Consumer<K, V> consumer, Duration forceWholeCommitInterval) {
        super(consumer);
        this.forceWholeCommitInterval = forceWholeCommitInterval;
        this.nextWholeCommitNanos = nextForceWholeCommitTime(forceWholeCommitInterval);
    }

    Map<TopicPartition, OffsetAndMetadata> offsetsToPartialCommit() {
        if (needWholeCommit()) {
            final Map<TopicPartition, OffsetAndMetadata> ret = new HashMap<>(completedTopicOffsets);
            for (TopicPartition partition : consumer.assignment()) {
                final OffsetAndMetadata offset = consumer.committed(partition);
                if (offset != null) {
                    ret.putIfAbsent(partition, offset);
                }
            }
            nextWholeCommitNanos = nextForceWholeCommitTime(forceWholeCommitInterval);
            return ret;
        } else {
            return completedTopicOffsets;
        }
    }

    private boolean needWholeCommit() {
        return System.nanoTime() >= nextWholeCommitNanos;
    }

    private long nextForceWholeCommitTime(Duration forceWholeCommitInterval) {
        return System.nanoTime() + forceWholeCommitInterval.toNanos();
    }
}
