package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A handler to handle all the consumer records consumed from Kafka broker.
 *
 * @param <K> the type of key for records consumed from Kafka
 * @param <V> the type of value for records consumed from Kafka
 */
public interface ConsumerRecordHandler<K, V> {
    /**
     * Handle a {@link ConsumerRecord} consumed from Kafka broker.
     * <p>
     * For the sake of fail fast, if any exception throws from this method, the thread for fetching records
     * from Kafka broker will exit with an error message. This will trigger rebalances of partitions
     * among all the consumer groups this consumer joined and let other consumer try to handle the failed and the
     * following records.
     * <p>
     * You can consider to use {@link RetriableConsumerRecordHandler} to retry handling process automatically.
     * Or use {@link CatchAllExceptionConsumerRecordHandler} as a safety net to handle all the failed records in
     * the same way.
     *
     * @param record the consumed {@link ConsumerRecord}
     */
    void handleRecord(ConsumerRecord<K, V> record);
}
