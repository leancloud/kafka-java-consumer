package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * A status of {@link LcKafkaConsumer} when it unsubscribed to all the topics that have subscribed to.
 */
public enum UnsubscribedStatus {
    /**
     * Indicate that the unsubscription is due to {@link LcKafkaConsumer#close()} was called.
     * Usually, this is an expected status.
     */
    CLOSED,

    /**
     * Indicate that the unsubscription is due to an error occurred, maybe an exception was thrown from
     * {@link ConsumerRecordHandler#handleRecord(ConsumerRecord)}, or some unrecoverable error happened
     * within {@link org.apache.kafka.clients.consumer.KafkaConsumer}. You can check the specific exception
     * in log.
     */
    ERROR
}
