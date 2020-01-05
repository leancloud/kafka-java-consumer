package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static cn.leancloud.kafka.consumer.BasicConsumerConfigs.ENABLE_AUTO_COMMIT;
import static java.util.Objects.requireNonNull;

/**
 * A builder used to create a {@link LcKafkaConsumer} which uses a {@link KafkaConsumer} to consume records
 * from Kafka broker.
 *
 * @param <K> the type of key for records consumed from Kafka
 * @param <V> the type of value for records consumed from Kafka
 */
public final class LcKafkaConsumerBuilder<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(LcKafkaConsumerBuilder.class);

    /**
     * Create a {@code LcKafkaConsumerBuilder} used to build {@link LcKafkaConsumer}.
     *
     * @param kafkaConfigs          the kafka configs for {@link KafkaConsumer}. Please refer
     *                              <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >this document</a> for
     *                              valid configurations.
     * @param consumerRecordHandler a {@link ConsumerRecordHandler} to handle the consumed record from kafka
     * @return a new {@code LcKafkaConsumerBuilder}
     * @throws NullPointerException when {@code kafkaConfigs} or {@code consumerRecordHandler} is null
     */
    public static <K, V> LcKafkaConsumerBuilder<K, V> newBuilder(Map<String, Object> kafkaConfigs,
                                                                 ConsumerRecordHandler<K, V> consumerRecordHandler) {
        requireNonNull(kafkaConfigs, "kafkaConfigs");
        requireNonNull(consumerRecordHandler, "consumerRecordHandler");
        return new LcKafkaConsumerBuilder<>(new HashMap<>(kafkaConfigs), consumerRecordHandler);
    }

    /**
     * Create a {@code LcKafkaConsumerBuilder} used to build {@link LcKafkaConsumer}.
     *
     * @param kafkaConfigs          the kafka configs for {@link KafkaConsumer}. Please refer
     *                              <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >this document</a> for
     *                              valid configurations.
     * @param consumerRecordHandler a {@link ConsumerRecordHandler} to handle the consumed record from kafka
     * @param keyDeserializer       the deserializer for key that implements {@link Deserializer}
     * @param valueDeserializer     the deserializer for value that implements {@link Deserializer}
     * @return a new {@code LcKafkaConsumerBuilder}
     * @throws NullPointerException when any of the input argument is null
     */
    public static <K, V> LcKafkaConsumerBuilder<K, V> newBuilder(Map<String, Object> kafkaConfigs,
                                                                 ConsumerRecordHandler<K, V> consumerRecordHandler,
                                                                 Deserializer<K> keyDeserializer,
                                                                 Deserializer<V> valueDeserializer) {
        requireNonNull(kafkaConfigs, "kafkaConfigs");
        requireNonNull(consumerRecordHandler, "consumerRecordHandler");
        requireNonNull(keyDeserializer, "keyDeserializer");
        requireNonNull(valueDeserializer, "valueDeserializer");
        return new LcKafkaConsumerBuilder<>(new HashMap<>(kafkaConfigs), consumerRecordHandler, keyDeserializer, valueDeserializer);
    }

    /**
     * Ensures that the argument expression is true.
     */
    private static void requireArgument(boolean expression, String template, Object... args) {
        if (!expression) {
            throw new IllegalArgumentException(String.format(template, args));
        }
    }

    private Duration pollTimeout = Duration.ofMillis(100);
    private int maxPendingAsyncCommits = 10;
    private Duration gracefulShutdownTimeout = Duration.ofSeconds(10);
    private ExecutorService workerPool = ImmediateExecutorService.INSTANCE;
    private boolean shutdownWorkerPoolOnStop = false;
    private Duration handleRecordTimeout = Duration.ZERO;
    private Map<String, Object> configs;
    private ConsumerRecordHandler<K, V> consumerRecordHandler;
    @Nullable
    private Consumer<K, V> consumer;
    @Nullable
    private Deserializer<K> keyDeserializer;
    @Nullable
    private Deserializer<V> valueDeserializer;
    @Nullable
    private CommitPolicy<K, V> policy;
    @Nullable
    // we have default value for recommitInterval but deliberately only initialize it to the default value in the
    // getter for recommitInterval. Because we would like to log a warning on user forget to set it
    private Duration recommitInterval;

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   ConsumerRecordHandler<K, V> consumerRecordHandler) {
        this(kafkaConsumerConfigs, consumerRecordHandler, null, null);
    }

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   ConsumerRecordHandler<K, V> consumerRecordHandler,
                                   @Nullable
                                           Deserializer<K> keyDeserializer,
                                   @Nullable
                                           Deserializer<V> valueDeserializer) {
        this.configs = kafkaConsumerConfigs;
        this.consumerRecordHandler = consumerRecordHandler;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }

    /**
     * The pollTimeout is the maximum time spent waiting in polling data from kafka broker if data is not available in
     * the buffer.
     * <p>
     * If 0, poll operation will return immediately with any records that are available currently in the buffer,
     * else returns empty.
     * <p>
     * Must not be negative. And the default {@code pollTimeoutMillis} is 100.
     *
     * @param pollTimeoutMillis the poll timeout in milliseconds
     * @return this
     * @throws IllegalArgumentException if {@code pollTimeoutMillis} is a negative value
     */
    public LcKafkaConsumerBuilder<K, V> pollTimeoutMillis(long pollTimeoutMillis) {
        requireArgument(pollTimeoutMillis >= 0, "pollTimeoutMillis: %s (expect >= 0)", pollTimeoutMillis);
        this.pollTimeout = Duration.ofMillis(pollTimeoutMillis);
        return this;
    }

    /**
     * The pollTimeout is the maximum time spent waiting in polling data from kafka broker if data is not available in
     * the buffer.
     * <p>
     * If 0, poll operation will return immediately with any records that are available currently in the buffer,
     * else returns empty.
     * <p>
     * The default {@code pollTimeout} is 100 millis seconds.
     *
     * @param pollTimeout the poll timeout duration
     * @return this
     * @throws NullPointerException     if {@code pollTimeout} is null
     * @throws IllegalArgumentException if {@code pollTimeout} is a negative duration
     */
    public LcKafkaConsumerBuilder<K, V> pollTimeout(Duration pollTimeout) {
        requireNonNull(pollTimeout, "pollTimeout");
        requireArgument(!pollTimeout.isNegative(), "pollTimeout: %s (expect positive or zero duration)", pollTimeout);
        this.pollTimeout = pollTimeout;
        return this;
    }

    /**
     * The maximum time spent in handling a single {@link org.apache.kafka.clients.consumer.ConsumerRecord} by calling
     * {@link ConsumerRecordHandler#handleRecord(ConsumerRecord)}. If the handling time for any {@code ConsumerRecord}
     * exceeds this limit, a {@link java.util.concurrent.TimeoutException} will be thrown which then will drag the
     * {@link LcKafkaConsumer} to shutdown. This mechanism is to prevent the "livelock" situation where it
     * seems the {@code LcKafkaConsumer} is OK, continuing on sending heartbeat and calling {@link Consumer#poll(long)},
     * but no progress is being made.
     * <p>
     * The default {@code handleRecordTimeoutMillis} is zero which means no limit on handling a {@code ConsumerRecord}.
     *
     * @param handleRecordTimeoutMillis the handle record timeout in millis seconds.
     * @return this
     * @throws IllegalArgumentException if {@code handleRecordTimeoutMillis} is a negative value
     */
    public LcKafkaConsumerBuilder<K, V> handleRecordTimeoutMillis(long handleRecordTimeoutMillis) {
        requireArgument(handleRecordTimeoutMillis >= 0,
                "handleRecordTimeoutMillis: %s (expect >= 0)", handleRecordTimeoutMillis);
        this.handleRecordTimeout = Duration.ofMillis(handleRecordTimeoutMillis);
        return this;
    }

    /**
     * The maximum time spent in handling a single {@link org.apache.kafka.clients.consumer.ConsumerRecord} by calling
     * {@link ConsumerRecordHandler#handleRecord(ConsumerRecord)}. If the handling time for any {@code ConsumerRecord}
     * exceeds this limit, a {@link java.util.concurrent.TimeoutException} will be thrown which then will drag the
     * {@link LcKafkaConsumer} to shutdown. This mechanism is to prevent the "livelock" situation where it
     * seems the {@code LcKafkaConsumer} is OK, continuing on sending heartbeat and calling {@link Consumer#poll(long)},
     * but no progress is being made.
     * <p>
     * The default {@code handleRecordTimeoutMillis} is zero which means no limit on handling a {@code ConsumerRecord}.
     *
     * @param handleRecordTimeout the handle record timeout duration.
     * @return this
     * @throws NullPointerException     if {@code handleRecordTimeout} is null
     * @throws IllegalArgumentException if {@code handleRecordTimeout} is a negative duration
     */
    public LcKafkaConsumerBuilder<K, V> handleRecordTimeout(Duration handleRecordTimeout) {
        requireNonNull(handleRecordTimeout, "handleRecordTimeout");
        requireArgument(!handleRecordTimeout.isNegative(),
                "handleRecordTimeout: %s (expect positive or zero duration)", handleRecordTimeout);
        this.handleRecordTimeout = handleRecordTimeout;
        return this;
    }

    /**
     * Sets the amount of time to wait after calling {@link LcKafkaConsumer#close()} for
     * consumed records to handle before actually shutting down.
     * <p>
     * The default {@code gracefulShutdownTimeoutMillis} is 10_000.
     *
     * @param gracefulShutdownTimeoutMillis the graceful shutdown timeout in milliseconds
     * @return this
     * @throws IllegalArgumentException if {@code gracefulShutdownTimeoutMillis} is a negative value
     */
    public LcKafkaConsumerBuilder<K, V> gracefulShutdownTimeoutMillis(long gracefulShutdownTimeoutMillis) {
        requireArgument(gracefulShutdownTimeoutMillis >= 0,
                "gracefulShutdownTimeoutMillis: %s (expected >= 0)", gracefulShutdownTimeoutMillis);
        this.gracefulShutdownTimeout = Duration.ofMillis(gracefulShutdownTimeoutMillis);
        return this;
    }

    /**
     * Sets the amount of time to wait after calling {@link LcKafkaConsumer#close()} for
     * consumed records to handle before actually shutting down.
     * <p>
     * The default {@code gracefulShutdownTimeout} is 10 seconds.
     *
     * @param gracefulShutdownTimeout the graceful shutdown timeout duration
     * @return this
     * @throws NullPointerException     if {@code gracefulShutdownTimeout} is null
     * @throws IllegalArgumentException if {@code gracefulShutdownTimeout} is a negative duration
     */
    public LcKafkaConsumerBuilder<K, V> gracefulShutdownTimeout(Duration gracefulShutdownTimeout) {
        requireNonNull(gracefulShutdownTimeout, "gracefulShutdownTimeout");
        requireArgument(!gracefulShutdownTimeout.isNegative(),
                "gracefulShutdownTimeout: %s (expect positive or zero duration)", gracefulShutdownTimeout);
        this.gracefulShutdownTimeout = gracefulShutdownTimeout;
        return this;
    }

    /**
     * When using async consumer to commit offset asynchronously, this argument can force consumer to do a synchronous
     * commit after there's already this ({@code maxPendingAsyncCommits}) many async commits on the fly without
     * response from broker.
     * <p>
     * The default {@code maxPendingAsyncCommits} is 10.
     *
     * @param maxPendingAsyncCommits do a synchronous commit when pending async commits beyond this limit
     * @return this
     * @throws IllegalArgumentException if {@code maxPendingAsyncCommits} is a non-positive value
     */
    public LcKafkaConsumerBuilder<K, V> maxPendingAsyncCommits(int maxPendingAsyncCommits) {
        requireArgument(maxPendingAsyncCommits > 0,
                "maxPendingAsyncCommits: %s (expect > 0)", maxPendingAsyncCommits);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        return this;
    }

    /**
     * The interval to commit all partitions and it's completed offsets to broker on a non-automatic commit consumer.
     * <p>
     * This configuration is only valid and is required on a non-automatic commit consumer build with
     * {@link LcKafkaConsumerBuilder#buildSync()}, {@link LcKafkaConsumerBuilder#buildAsync()},
     * {@link LcKafkaConsumerBuilder#buildPartialSync()} or {@link LcKafkaConsumerBuilder#buildPartialAsync()}.
     * For these kind of consumers, usually they only commit offsets of a partition when there was records consumed from
     * that partition and all these consumed records was handled successfully. But we must periodically commit those
     * subscribed partitions who have had records but no new records for a long time too. Otherwise, after commit offset
     * retention timeout, Kafka broker may forget where the current commit offset of these partition for the consumer
     * are. Then, when the consumer crashed and recovered, if the consumer set <code>auto.offset.reset</code>
     * configuration to <code>earliest</code>, it may consume a already consumed record again. So please make sure
     * that {@code recommitIntervalInMillis} is within the limit set by <code>offsets.retention.minutes</code>
     * on Kafka broker or even within 1/3 of that limit to tolerate some commit failures on async commit consumer.
     * <p>
     * The default {@code recommitInterval} is 1 hour.
     *
     * @param recommitIntervalInMillis the interval in millis seconds to do a recommit
     * @return this
     * @throws IllegalArgumentException if {@code recommitIntervalInMillis} is a non-positive value
     */
    public LcKafkaConsumerBuilder<K, V> recommitIntervalInMillis(long recommitIntervalInMillis) {
        requireArgument(recommitIntervalInMillis > 0,
                "recommitIntervalInMillis: %s (expected > 0)", recommitIntervalInMillis);

        this.recommitInterval = Duration.ofMillis(recommitIntervalInMillis);
        return this;
    }

    /**
     * The interval to commit all partitions and it's completed offsets to broker on a non-automatic commit consumer.
     * <p>
     * This configuration is only valid and is required on a non-automatic commit consumer build with
     * {@link LcKafkaConsumerBuilder#buildSync()}, {@link LcKafkaConsumerBuilder#buildAsync()},
     * {@link LcKafkaConsumerBuilder#buildPartialSync()} or {@link LcKafkaConsumerBuilder#buildPartialAsync()}.
     * For these kind of consumers, usually they only commit offsets of a partition when there was records consumed from
     * that partition and all these consumed records was handled successfully. But we must periodically commit those
     * subscribed partitions who have had records but no new records for a long time too. Otherwise, after commit offset
     * retention timeout, Kafka broker may forget where the current commit offset of these partition for the consumer
     * are. Then, when the consumer crashed and recovered, if the consumer set <code>auto.offset.reset</code>
     * configuration to <code>earliest</code>, it may consume a already consumed record again. So please make sure
     * that {@code recommitInterval} is within the limit set by <code>offsets.retention.minutes</code> on
     * Kafka broker or even within 1/3 of that limit to tolerate some commit failures on async commit consumer..
     * <p>
     * The default {@code recommitInterval} is 1 hour.
     *
     * @param recommitInterval the interval to do a recommit
     * @return this
     * @throws NullPointerException     if {@code recommitInterval} is null
     * @throws IllegalArgumentException if {@code recommitInterval} is a non-positive duration
     */
    public LcKafkaConsumerBuilder<K, V> recommitInterval(Duration recommitInterval) {
        requireNonNull(recommitInterval, "recommitInterval");
        requireArgument(!recommitInterval.isNegative() && !recommitInterval.isZero(),
                "recommitInterval: %s (expect positive duration)", recommitInterval);
        this.recommitInterval = recommitInterval;
        return this;
    }

    /**
     * Internal testing usage only.
     * <p>
     * Passing a {@link Consumer} as the underlying {@link Consumer}. Usually this would be a {@link MockConsumer}.
     *
     * @param mockedConsumer the injected consumer
     * @return this
     * @throws NullPointerException if {@code mockedConsumer} is null
     */
    LcKafkaConsumerBuilder<K, V> mockKafkaConsumer(Consumer<K, V> mockedConsumer) {
        requireNonNull(mockedConsumer, "consumer");
        if (mockedConsumer instanceof KafkaConsumer) {
            throw new IllegalArgumentException("need a mocked Consumer");
        }
        this.consumer = mockedConsumer;
        return this;
    }

    /**
     * The thread pool used by consumer to handle the consumed records from Kafka broker. If no worker pool is provided,
     * the created {@link LcKafkaConsumer} will use {@link ImmediateExecutorService} to handle records in
     * the records polling thread instead.
     * <p>
     * When a worker pool is provided, after each poll, the polling thread will take one thread from this worker pool
     * for each polled {@link org.apache.kafka.clients.consumer.ConsumerRecord} to handle the record. Please tune
     * the <code>max.poll.records</code> in kafka configs to limit the number of records polled at each time do not
     * exceed the max size of the provided worker thread pool. Otherwise, a
     * {@link java.util.concurrent.RejectedExecutionException} will thrown when the polling thread submitting too much
     * tasks to the pool. Then this exception will lead the only polling thread to exit.
     * <p>
     * If you are using partial sync/async commit consumer by building {@link LcKafkaConsumer} with
     * {@link LcKafkaConsumerBuilder#buildPartialSync()} or {@link LcKafkaConsumerBuilder#buildPartialAsync()}, without
     * a worker pool, they degrade to sync/async commit consumer as built with {@link LcKafkaConsumerBuilder#buildSync()}
     * or {@link LcKafkaConsumerBuilder#buildAsync()}.
     * <p>
     * If no worker pool provided, you also need to tune {@code max.poll.interval.ms} in kafka configs, to ensure the
     * polling thread can at least poll once within {@code max.poll.interval.ms} during handling consumed messages
     * to prevent itself from session timeout or polling timeout.
     *
     * @param workerPool     a thread pool to handle consumed records
     * @param shutdownOnStop true to shutdown the input worker pool when this consumer closed
     * @return this
     * @throws NullPointerException if {@code workerPool} is null
     */
    public LcKafkaConsumerBuilder<K, V> workerPool(ExecutorService workerPool, boolean shutdownOnStop) {
        requireNonNull(workerPool, "workerPool");
        this.workerPool = workerPool;
        this.shutdownWorkerPoolOnStop = shutdownOnStop;
        return this;
    }

    /**
     * Build a consumer which commits offset automatically at fixed interval. It is both OK for with or without a
     * worker thread pool. But without a worker pool, please tune the {@code max.poll.interval.ms} in
     * Kafka configs as mentioned in {@link LcKafkaConsumerBuilder#workerPool(ExecutorService, boolean)}.
     * <p>
     * This kind of consumer requires the following kafka configs must be set, otherwise
     * {@link IllegalArgumentException} will be thrown:
     * <ol>
     *  <li><code>max.poll.interval.ms</code></li>
     *  <li><code>max.poll.records</code></li>
     *  <li><code>auto.offset.reset</code></li>
     *  <li><code>auto.commit.interval.ms</code></li>
     * </ol>
     * <p>
     * Though all of these configs have default values in kafka, we still require every user to set them specifically.
     * Because these configs is vital for using this consumer safely.
     * <p>
     * If you set {@code enable.auto.commit} to false, this consumer will set it to true by itself.
     *
     * @return this
     * @throws IllegalArgumentException if any required kafka consumer configurations is invalid or not provided
     */
    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAuto() {
        checkConfigs(AutoCommitConsumerConfigs.values());
        consumer = buildConsumer(true);
        policy = workerPool == ImmediateExecutorService.INSTANCE ? NoOpCommitPolicy.getInstance() : new AutoCommitPolicy<>(consumer);
        return doBuild();
    }

    /**
     * Build a consumer in which the polling thread always does a sync commit after all the polled records has been handled.
     * Because it only commits after all the polled records handled, so the longer the records handling process，
     * the longer the interval between each commits, the bigger of the possibility to repeatedly consume a same record
     * when the consumer crash.
     * <p>
     * This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
     * consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
     * otherwise an {@link IllegalArgumentException} will be thrown:
     * <ol>
     *  <li><code>max.poll.records</code></li>
     *  <li><code>auto.offset.reset</code></li>
     * </ol>
     * <p>
     * Though all of these configs have default values in kafka, we still require every user to set them specifically.
     * Because these configs is vital for using this consumer safely.
     * <p>
     * If you set {@code enable.auto.commit} to true, this consumer will set it to false by itself.
     *
     * @return this
     * @throws IllegalArgumentException if any required kafka consumer configurations is invalid or not provided
     */
    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildSync() {
        consumer = buildConsumer(false);
        policy = new SyncCommitPolicy<>(consumer, getRecommitInterval());
        return doBuild();
    }

    /**
     * Build a consumer in which the polling thread does a sync commits whenever there's any handled consumer records. It
     * commits often, so after a consumer crash, comparatively little records may be handled more than once. But also
     * due to commit often, the overhead causing by committing is relatively high.
     * <p>
     * This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
     * consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
     * otherwise an {@link IllegalArgumentException} will be thrown:
     * <ol>
     *  <li><code>max.poll.records</code></li>
     *  <li><code>auto.offset.reset</code></li>
     * </ol>
     * <p>
     * Though all of these configs have default values in kafka, we still require every user to set them specifically.
     * Because these configs is vital for using this consumer safely.
     * <p>
     * If you set {@code enable.auto.commit} to true, this consumer will set it to false by itself.
     *
     * @return this
     * @throws IllegalArgumentException if any required kafka consumer configurations is invalid or not provided
     */
    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialSync() {
        consumer = buildConsumer(false);
        policy = new PartialSyncCommitPolicy<>(consumer, getRecommitInterval());
        return doBuild();
    }

    /**
     * Build a consumer in which the polling thread always does a async commit after all the polled records has been handled.
     * Because it only commits after all the polled records handled, so the longer the records handling process，
     * the longer the interval between each commits, the bigger of the possibility to repeatedly consume a same record
     * when the consumer crash.
     * <p>
     * If any async commit is failed or the number of pending async commits is beyond the limit set by
     * {@link LcKafkaConsumerBuilder#maxPendingAsyncCommits(int)}, this consumer will do a sync commit to commit all the
     * records which have been handled.
     * <p>
     * This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
     * consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
     * otherwise an {@link IllegalArgumentException} will be thrown:
     * <ol>
     *  <li><code>max.poll.records</code></li>
     *  <li><code>auto.offset.reset</code></li>
     * </ol>
     * <p>
     * Though all of these configs have default values in kafka, we still require every user to set them specifically.
     * Because these configs is vital for using this consumer safely.
     * <p>
     * If you set {@code enable.auto.commit} to true, this consumer will set it to false by itself.
     *
     * @return this
     * @throws IllegalArgumentException if any required kafka consumer configurations is invalid or not provided
     */
    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAsync() {
        consumer = buildConsumer(false);
        policy = new AsyncCommitPolicy<>(consumer, getRecommitInterval(), maxPendingAsyncCommits);
        return doBuild();
    }

    /**
     * Build a consumer in which the polling thread does a async commits whenever there's any handled consumer records. It
     * commits often, so after a consumer crash, comparatively little records may be handled more than once. It use
     * async commit to mitigate the overhead causing by high committing times.
     * <p>
     * If any async commit is failed or the number of pending async commits is beyond the limit set by
     * {@link LcKafkaConsumerBuilder#maxPendingAsyncCommits(int)}, this consumer will do a sync commit to commit all the
     * records which have been handled.
     * <p>
     * This kind of consumer ensures to do a sync commit to commit all the finished records at that time when the
     * consumer is shutdown or any partition was revoked. It requires the following kafka configs must be set,
     * otherwise an {@link IllegalArgumentException} will be thrown:
     * <ol>
     *  <li><code>max.poll.records</code></li>
     *  <li><code>auto.offset.reset</code></li>
     * </ol>
     * <p>
     * Though all of these configs have default values in kafka, we still require every user to set them specifically.
     * Because these configs is vital for using this consumer safely.
     * <p>
     * If you set {@code enable.auto.commit} to true, this consumer will set it to false by itself.
     *
     * @return this
     * @throws IllegalArgumentException if any required kafka consumer configurations is invalid or not provided
     */
    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialAsync() {
        consumer = buildConsumer(false);
        policy = new PartialAsyncCommitPolicy<>(consumer, getRecommitInterval(), maxPendingAsyncCommits);
        return doBuild();
    }

    Consumer<K, V> getConsumer() {
        assert consumer != null;
        return consumer;
    }

    ConsumerRecordHandler<K, V> getConsumerRecordHandler() {
        return consumerRecordHandler;
    }

    ExecutorService getWorkerPool() {
        return workerPool;
    }

    boolean isShutdownWorkerPoolOnStop() {
        return shutdownWorkerPoolOnStop;
    }

    Duration getPollTimeout() {
        return pollTimeout;
    }

    Duration gracefulShutdownTimeout() {
        return gracefulShutdownTimeout;
    }

    Duration handleRecordTimeout() {
        return handleRecordTimeout;
    }

    CommitPolicy<K, V> getPolicy() {
        assert policy != null;
        return policy;
    }

    private Consumer<K, V> buildConsumer(boolean autoCommit) {
        checkConfigs(BasicConsumerConfigs.values());
        ENABLE_AUTO_COMMIT.set(configs, Boolean.toString(autoCommit));
        if (keyDeserializer != null) {
            assert valueDeserializer != null;
        } else {
            assert valueDeserializer == null;
        }

        if (consumer != null) {
            // if consumer exists, it must be a mocked consumer, not KafkaConsumer
            assert !(consumer instanceof KafkaConsumer);
            return consumer;
        }
        return new KafkaConsumer<>(configs, keyDeserializer, valueDeserializer);
    }

    private Duration getRecommitInterval() {
        if (recommitInterval == null) {
            logger.warn("Recommit interval is not set for a non-automatic commit consumer, the default " +
                    "interval of 1 hour will be used.");
            recommitInterval = Duration.ofHours(1);
        }

        return recommitInterval;
    }

    private void checkConfigs(KafkaConfigsChecker[] checkers) {
        for (KafkaConfigsChecker check : checkers) {
            check.check(configs);
        }
    }

    private <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> doBuild() {
        @SuppressWarnings("unchecked")
        final LcKafkaConsumer<K1, V1> c = (LcKafkaConsumer<K1, V1>) new LcKafkaConsumer<>(this);
        return c;
    }
}
