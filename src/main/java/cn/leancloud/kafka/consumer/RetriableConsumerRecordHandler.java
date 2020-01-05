package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

/**
 * A wrapper over a {@link ConsumerRecordHandler} to let the wrapped {@code ConsumerRecordHandler} try to handle a
 * record in a limited times in case the handling process failed.
 *
 * @param <K> the type of key for records consumed from Kafka
 * @param <V> the type of value for records consumed from Kafka
 */
public final class RetriableConsumerRecordHandler<K, V> implements ConsumerRecordHandler<K, V> {
    private final int maxRetryTimes;
    private final Duration retryInterval;
    private final ConsumerRecordHandler<K, V> wrappedHandler;

    /**
     * Constructor for {@code RetriableConsumerRecordHandler} with max retry times limit. The returned
     * {@code RetriableConsumerRecordHandler} is a {@link ConsumerRecordHandler} which will retry to handle a
     * {@link ConsumerRecord} automatically if it was failed, until the {@link ConsumerRecord} was handled successfully
     * or retried times exceeded {@code maxRetryTimes} after which a {@link HandleMessageFailedException} will be thrown.
     *
     * @param wrappedHandler the wrapped {@link ConsumerRecordHandler}.
     * @param maxRetryTimes  maximum retry times.
     * @throws NullPointerException     when {@code wrappedHandler} is null
     * @throws IllegalArgumentException if {@code maxRetryTimes} is a negative value
     */
    public RetriableConsumerRecordHandler(ConsumerRecordHandler<K, V> wrappedHandler, int maxRetryTimes) {
        this(wrappedHandler, maxRetryTimes, Duration.ZERO);
    }

    /**
     * Constructor for {@code RetriableConsumerRecordHandler} with max retry times limit and retry interval. The returned
     * {@code RetriableConsumerRecordHandler} is a {@link ConsumerRecordHandler} which will retry to handle a
     * {@link ConsumerRecord} automatically if it was failed, until the {@link ConsumerRecord} was handled successfully
     * or retried times exceeded {@code maxRetryTimes} after which a {@link HandleMessageFailedException} will be thrown.
     *
     * @param wrappedHandler the wrapped {@link ConsumerRecordHandler}.
     * @param maxRetryTimes  maximum retry times.
     * @param retryInterval  the interval between every retry
     * @throws NullPointerException     when {@code wrappedHandler} or {@code retryInterval} is null
     * @throws IllegalArgumentException if {@code maxRetryTimes} is a negative value or retryInterval is a negative duration
     */
    public RetriableConsumerRecordHandler(ConsumerRecordHandler<K, V> wrappedHandler, int maxRetryTimes, Duration retryInterval) {
        requireNonNull(wrappedHandler, "wrappedHandler");
        if (maxRetryTimes <= 0) {
            throw new IllegalArgumentException("maxRetryTimes: " + maxRetryTimes + " (expect > 0)");
        }
        requireNonNull(retryInterval, "retryInterval");
        if (retryInterval.isNegative()) {
            throw new IllegalArgumentException("retryInterval: " + retryInterval + " (expect positive duration)");
        }

        this.maxRetryTimes = maxRetryTimes;
        this.wrappedHandler = wrappedHandler;
        this.retryInterval = retryInterval;
    }

    @Override
    public void handleRecord(ConsumerRecord<K, V> record) {
        Exception lastException = null;
        int retried = 0;
        while (retried <= maxRetryTimes) {
            try {
                wrappedHandler.handleRecord(record);
                return;
            } catch (Exception ex) {
                lastException = ex;
                try {
                    Thread.sleep(retryInterval.toMillis());
                } catch (InterruptedException e) {
                    // keep the interrupt status and still retry for the next time
                    // because interrupt means we can't block this thread, but
                    // it does not mean we should quit our job
                    Thread.currentThread().interrupt();
                }
                ++retried;
            }
        }

        if (lastException != null) {
            throw new HandleMessageFailedException(lastException);
        }
    }
}
