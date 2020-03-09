package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptySet;

final class AsyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private final OffsetCommitCallback callback;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    AsyncCommitPolicy(Consumer<K, V> consumer,
                      Duration syncCommitRetryInterval,
                      int maxAttemptsForEachSyncCommit,
                      Duration recommitInterval,
                      int maxPendingAsyncCommits) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, recommitInterval);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        this.callback = new AsyncCommitCallback();
    }

    @Override
    Set<TopicPartition> tryCommit0(boolean noPendingRecords) {
        // with forceSync mark it means a previous async commit was failed, so
        // we do a sync commit no matter if there's any pending records or completed offsets
        if (!forceSync && (!noPendingRecords || noTopicOffsetsToCommit())) {
            return emptySet();
        }

        final Set<TopicPartition> partitions = partitionsForAllRecordsStates();
        commit();

        // for our commit policy, no matter syncCommit or asyncCommit we are using, we always
        // commit all assigned offsets, so we can update recommit time here safely. And
        // we don't mind that if the async commit request failed, we tolerate this situation
        updateNextRecommitTime();

        return partitions;
    }

    @VisibleForTesting
    int pendingAsyncCommitCount() {
        return pendingAsyncCommitCounter;
    }

    @VisibleForTesting
    boolean forceSync() {
        return forceSync;
    }

    private void commit() {
        if (forceSync || pendingAsyncCommitCounter >= maxPendingAsyncCommits) {
            commitSyncWithRetry();
            pendingAsyncCommitCounter = 0;
            forceSync = false;
            clearAllProcessingRecordStates();
        } else {
            ++pendingAsyncCommitCounter;
            consumer.commitAsync(callback);
        }
    }

    private class AsyncCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            --pendingAsyncCommitCounter;
            assert pendingAsyncCommitCounter >= 0 : "actual: " + pendingAsyncCommitCounter;
            if (exception != null) {
                logger.warn("Failed to commit offsets: " + offsets + " asynchronously", exception);
                forceSync = true;
            } else {
                clearProcessingRecordStatesForCompletedPartitions(offsets);
            }
        }
    }
}
