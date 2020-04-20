package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;

interface CommitPolicy {
    /**
     * Try commit offset for any {@link TopicPartition}s which has processed {@link ConsumerRecord}s based on the
     * intrinsic policy of this {@link CommitPolicy}. This method is called whenever there're any
     * {@link ConsumerRecord} processed.
     *
     * @param noPendingRecords is there any pending records which have not been processed. Though {@link CommitPolicy}
     *                         can calculate this value by itself, we still pass this value as {@link Fetcher} can
     *                         calculate this value much quicker
     * @return those {@link TopicPartition}s which have no pending {@code ConsumerRecord}s
     */
    Set<TopicPartition> tryCommit(boolean noPendingRecords, ProcessRecordsProgress progress);

    /**
     * Do a dedicated partition commit synchronously which only commit those {@link ConsumerRecord}s that have
     * processed but have not been committed yet. Usually it is called when {@link LcKafkaConsumer} is about to
     * shutdown or when some partitions was revoked.
     *
     * @return those {@link TopicPartition}s which have no pending {@code ConsumerRecord}s
     */
    Set<TopicPartition> partialCommitSync(ProcessRecordsProgress progress);

    /**
     * Pause commit until {@link #resumeCommit()} is called. Next {@link #tryCommit(boolean, ProcessRecordsProgress)}
     * will return empty Set without commit anything.
     * <p>
     * Please note that this method has no effect on {@link #partialCommitSync(ProcessRecordsProgress)} which will
     * even this method was called.
     */
    void pauseCommit();

    /**
     * Resume commit and next {@link #tryCommit(boolean, ProcessRecordsProgress)} will do the commitment stuff.
     */
    void resumeCommit();

    /**
     * Check weather commit was paused.
     *
     * @return true when {@link #pauseCommit()} has been called
     */
    boolean commitPaused();
}
