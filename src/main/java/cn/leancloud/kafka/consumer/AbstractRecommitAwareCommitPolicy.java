package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

abstract class AbstractRecommitAwareCommitPolicy<K, V> extends AbstractCommitPolicy<K, V> {
    private final Duration recommitInterval;
    private long nextRecommitNanos;

    AbstractRecommitAwareCommitPolicy(Consumer<K, V> consumer,
                                      Duration syncCommitRetryInterval,
                                      int maxAttemptsForEachSyncCommit,
                                      Duration recommitInterval) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit);
        this.recommitInterval = recommitInterval;
        updateNextRecommitTime(System.nanoTime());
    }

    @Override
    public final Set<TopicPartition> tryCommit(boolean noPendingRecords, ProcessRecordsProgress progress) {
        if (commitPaused()) {
            return Collections.emptySet();
        }

        if (needRecommit()) {
            commitSyncWithRetry(offsetsForRecommit());
            updateNextRecommitTime();
        }
        return tryCommit0(noPendingRecords, progress);
    }

    abstract Set<TopicPartition> tryCommit0(boolean noPendingRecords, ProcessRecordsProgress progress);

    void updateNextRecommitTime() {
        updateNextRecommitTime(System.nanoTime());
    }

    @VisibleForTesting
    void updateNextRecommitTime(long currentNanos) {
        nextRecommitNanos = currentNanos + recommitInterval.toNanos();
    }

    @VisibleForTesting
    long nextRecommitNanos() {
        return nextRecommitNanos;
    }

    private boolean needRecommit() {
        return System.nanoTime() >= nextRecommitNanos;
    }

    private Map<TopicPartition, OffsetAndMetadata> offsetsForRecommit() {
        assert needRecommit() : "current nanos: " + System.nanoTime() + " nextRecommitNanos:" + nextRecommitNanos;

        final Map<TopicPartition, OffsetAndMetadata> ret = new HashMap<>();
        for (TopicPartition partition : consumer.assignment()) {
            final OffsetAndMetadata offset = consumer.committed(partition);
            if (offset != null) {
                ret.put(partition, offset);
            }
        }

        return ret;
    }
}
