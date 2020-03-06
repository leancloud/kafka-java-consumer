package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Set;

interface CommitPolicy<K, V> {
    /**
     * Mark an {@link ConsumerRecord} as pending before processing it. So {@link CommitPolicy} can know which and
     * how many records we need to process. It is called by {@link Fetcher} when {@code Fetcher} fetched any
     * {@link ConsumerRecord}s from Broker.
     *
     * @param record the {@link ConsumerRecord} need to process
     */
    void markPendingRecord(ConsumerRecord<K, V> record);

    /**
     * Mark an {@link ConsumerRecord} as completed after processing it. So {@link CommitPolicy} can know which and
     * how many records we have processed. It is called by {@link Fetcher} when {@code Fetcher} make sure that
     * a {@code ConsumerRecord} was processed successfully.
     *
     * @param record the {@link ConsumerRecord} processed
     */
    void markCompletedRecord(ConsumerRecord<K, V> record);

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
    Set<TopicPartition> tryCommit(boolean noPendingRecords);

    /**
     * Do a dedicated partition commit synchronously which only commit those {@link ConsumerRecord}s that have
     * processed but have not been committed yet. Usually it is called when {@link LcKafkaConsumer} is about to
     * shutdown or when some partitions was revoked.
     *
     * @return those {@link TopicPartition}s which have no pending {@code ConsumerRecord}s
     */
    Set<TopicPartition> partialCommitSync();

    /**
     * Revoke internal states for some partitions.
     *
     * @param partitions which was revoked from consumer
     */
    void revokePartitions(Collection<TopicPartition> partitions);
}
