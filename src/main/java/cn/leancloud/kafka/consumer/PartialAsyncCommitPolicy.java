package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static java.util.Collections.emptySet;

final class PartialAsyncCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(PartialAsyncCommitPolicy.class);

    private final int maxPendingAsyncCommits;
    private final OffsetCommitCallback fullCommitCallback;
    private final OffsetCommitCallback partialCommitCallback;
    private final Map<TopicPartition, OffsetAndMetadata> pendingAsyncCommitOffset;
    private int pendingAsyncCommitCounter;
    private boolean forceSync;

    PartialAsyncCommitPolicy(Consumer<K, V> consumer,
                             Duration syncCommitRetryInterval,
                             int maxAttemptsForEachSyncCommit,
                             Duration forceWholeCommitInterval,
                             int maxPendingAsyncCommits) {
        super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, forceWholeCommitInterval);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        this.fullCommitCallback = new AsyncFullCommitCallback();
        this.partialCommitCallback = new AsyncPartialCommitCallback();
        this.pendingAsyncCommitOffset = new HashMap<>();
    }

    @Override
    Set<TopicPartition> tryCommit0(boolean noPendingRecords) {
        if (forceSync) {
            return tryCommitOnForceSync(noPendingRecords);
        }

        if (noTopicOffsetsToCommit()) {
            return emptySet();
        }

        if (noPendingRecords) {
            return fullCommit();
        }

        final Set<TopicPartition> completePartitions;
        if (useSyncCommit()) {
            completePartitions = partialCommitSync();
            pendingAsyncCommitOffset.clear();
            pendingAsyncCommitCounter = 0;
        } else {
            final Map<TopicPartition, OffsetAndMetadata> offsets = completedTopicOffsetsToCommit();
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : pendingAsyncCommitOffset.entrySet()) {
                offsets.remove(entry.getKey(), entry.getValue());
            }

            if (offsets.isEmpty()) {
                return emptySet();
            }

            ++pendingAsyncCommitCounter;

            completePartitions = partitionsToSafeResume(offsets);
            pendingAsyncCommitOffset.putAll(offsets);
            consumer.commitAsync(offsets, partialCommitCallback);
        }
        return completePartitions;
    }

    @VisibleForTesting
    int pendingAsyncCommitCount() {
        return pendingAsyncCommitCounter;
    }

    @VisibleForTesting
    void setPendingAsyncCommitCount(int count) {
        pendingAsyncCommitCounter = count;
    }

    @VisibleForTesting
    Map<TopicPartition, OffsetAndMetadata> pendingAsyncCommitOffset() {
        return pendingAsyncCommitOffset;
    }

    @VisibleForTesting
    boolean forceSync() {
        return forceSync;
    }

    @VisibleForTesting
    void setForceSync(boolean forceSync) {
        this.forceSync = forceSync;
    }

    private Set<TopicPartition> tryCommitOnForceSync(boolean noPendingRecords) {
        final Set<TopicPartition> completedPartitions;
        if (noPendingRecords) {
            completedPartitions = fullCommitSync();
            updateNextRecommitTime();
        } else {
            completedPartitions = partialCommitSync();
        }
        pendingAsyncCommitOffset.clear();
        pendingAsyncCommitCounter = 0;
        forceSync = false;
        return completedPartitions;
    }

    private boolean useSyncCommit() {
        return pendingAsyncCommitCounter >= maxPendingAsyncCommits;
    }

    private Set<TopicPartition> fullCommit() {
        final Set<TopicPartition> completePartitions = partitionsForAllRecordsStates();
        if (useSyncCommit()) {
            commitSyncWithRetry();
            pendingAsyncCommitCounter = 0;
        } else {
            ++pendingAsyncCommitCounter;
            consumer.commitAsync(fullCommitCallback);
        }
        // no matter sync or async commit we are using, we always
        // commit all assigned offsets, so we can update recommit time here safely. And
        // we don't mind that if the async commit request failed, we tolerate this situation
        updateNextRecommitTime();

        // we can clear records states even though the async commit may fail. because if
        // it did failed, we will do a sync commit on next try commit
        clearAllProcessingRecordStates();
        pendingAsyncCommitOffset.clear();
        return completePartitions;
    }

    private class AsyncFullCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            --pendingAsyncCommitCounter;
            assert pendingAsyncCommitCounter >= 0 : "actual: " + pendingAsyncCommitCounter;
            if (exception != null) {
                // if last async commit is failed, we do not clean cached completed offsets and let next
                // commit be a sync commit so all the complete offsets will be committed at that time
                logger.warn("Failed to commit offset: " + offsets + " asynchronously", exception);
                forceSync = true;
            } else {
                clearProcessingRecordStatesForCompletedPartitions(offsets);
            }
        }
    }

    private class AsyncPartialCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            --pendingAsyncCommitCounter;
            assert pendingAsyncCommitCounter >= 0 : "actual: " + pendingAsyncCommitCounter;
            if (exception != null) {
                // if last async commit is failed, we do not clean cached completed offsets and let next
                // commit be a sync commit so all the complete offsets will be committed at that time
                logger.warn("Failed to commit offset: " + offsets + " asynchronously", exception);
                forceSync = true;
            } else {
                updatePartialCommittedOffsets(offsets);
                clearProcessingRecordStatesForCompletedPartitions(offsets);
            }
        }
    }
}
