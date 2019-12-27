package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.serialization.Deserializer;

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
    /**
     * Create a {@code LcKafkaConsumerBuilder} used to build {@link LcKafkaConsumer}.
     *
     * @param kafkaConfigs   the kafka configs for {@link KafkaConsumer}. Please refer
     *                       <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >this document</a> for
     *                       valid configurations.
     * @param consumerRecordHandler a {@link ConsumerRecordHandler} to handle the consumed record from kafka
     * @return a new {@code LcKafkaConsumerBuilder}
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
     * @param kafkaConfigs      the kafka configs for {@link KafkaConsumer}. Please refer
     *                          <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >this document</a> for
     *                          valid configurations.
     * @param consumerRecordHandler    a {@link ConsumerRecordHandler} to handle the consumed record from kafka
     * @param keyDeserializer   The deserializer for key that implements {@link Deserializer}
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}
     * @return a new {@code LcKafkaConsumerBuilder}
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

    private long pollTimeout = 100;
    private int maxPendingAsyncCommits = 10;
    private long gracefulShutdownMillis = 10_000;
    private ExecutorService workerPool = ImmediateExecutorService.INSTANCE;
    private boolean shutdownWorkerPoolOnStop = false;
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
     * Must not be negative.
     *
     * @param pollTimeoutMs the poll timeout in milliseconds
     * @return this
     */
    public LcKafkaConsumerBuilder<K, V> pollTimeoutMillis(long pollTimeoutMs) {
        requireArgument(pollTimeoutMs >= 0, "pollTimeoutMillis: %s (expect >= 0)", pollTimeoutMs);
        this.pollTimeout = pollTimeoutMs;
        return this;
    }

    /**
     * The pollTimeout is the maximum time spent waiting in polling data from kafka broker if data is not available in
     * the buffer.
     * <p>
     * If 0, poll operation will return immediately with any records that are available currently in the buffer,
     * else returns empty.
     * <p>
     * Must not be negative.
     *
     * @param pollTimeout the poll timeout duration
     * @return this
     */
    public LcKafkaConsumerBuilder<K, V> pollTimeout(Duration pollTimeout) {
        requireNonNull(pollTimeout, "pollTimeout");
        this.pollTimeout = pollTimeout.toMillis();
        return this;
    }

    public LcKafkaConsumerBuilder<K, V> gracefulShutdownTimeoutMillis(long gracefulShutdownMs) {
        requireArgument(gracefulShutdownMs >= 0, "gracefulShutdownMillis: %s (expected >= 0)", gracefulShutdownMs);
        this.gracefulShutdownMillis = gracefulShutdownMs;
        return this;
    }

    public LcKafkaConsumerBuilder<K, V> gracefulShutdownTimeout(Duration gracefulShutdownTimeout) {
        requireNonNull(gracefulShutdownTimeout, "gracefulShutdownTimeout");
        this.gracefulShutdownMillis = gracefulShutdownTimeout.toMillis();
        return this;
    }

    /**
     * When using async consumer to commit offset asynchronously, this argument can force consumer to do a synchronous
     * commit after there's already {@code maxPendingAsyncCommits} async commits on the fly without response from broker.
     *
     * @param maxPendingAsyncCommits do a synchronous commit when pending async commits beyond this limit
     * @return this
     */
    public LcKafkaConsumerBuilder<K, V> maxPendingAsyncCommits(int maxPendingAsyncCommits) {
        requireArgument(maxPendingAsyncCommits > 0,
                "maxPendingAsyncCommits: %s (expect > 0)", maxPendingAsyncCommits);
        this.maxPendingAsyncCommits = maxPendingAsyncCommits;
        return this;
    }

    /**
     * Change the {@link ConsumerRecordHandler} to handle the consumed record from kafka.
     *
     * @param consumerRecordHandler the handler to handle consumed record
     * @return this
     */
    public LcKafkaConsumerBuilder<K, V> messageHandler(ConsumerRecordHandler<K, V> consumerRecordHandler) {
        requireNonNull(consumerRecordHandler, "consumerRecordHandler");
        this.consumerRecordHandler = consumerRecordHandler;
        return this;
    }

    /**
     * Internal testing usage only.
     * Passing a {@link Consumer} as as the underlying kafka consumer. Usually this would be a {@link MockConsumer}.
     *
     * @return this
     */
    LcKafkaConsumerBuilder<K, V> mockKafkaConsumer(Consumer<K, V> consumer) {
        requireNonNull(consumer, "cn/leancloud/kafka/consumer");
        if (consumer instanceof KafkaConsumer) {
            throw new IllegalArgumentException("need a mocked Consumer");
        }
        this.consumer = consumer;
        return this;
    }

    /**
     * The thread pool used by consumer to handle the consumed messages from kafka. Please note that if you are
     * using auto commit consumer, this thread pool is not be used.
     *
     * @param workerPool     a thread pool to handle consumed messages
     * @param shutdownOnStop true to shutdown the input worker pool when this consumer closed
     * @return this
     */
    public LcKafkaConsumerBuilder<K, V> workerPool(ExecutorService workerPool, boolean shutdownOnStop) {
        requireNonNull(workerPool, "workerPool");
        this.workerPool = workerPool;
        this.shutdownWorkerPoolOnStop = shutdownOnStop;
        return this;
    }

    /**
     * Build a consumer which commit offset automatically in fixed interval. This consumer will not using other thread
     * pool to handle consumed messages and will not pause any partition when after polling. This consumer is
     * equivalent to:
     * <pre>
     * while (true) {
     *     final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);
     *     for (ConsumerRecord<K, V> record : records) {
     *         handler.handleRecord(record.topic(), record.value());
     *     }
     * }
     * </pre>
     *
     * <p>
     * Please note that this consumer requires these kafka configs must be set, otherwise
     * {@link IllegalArgumentException} will be thrown:
     * <ol>
     * <li><code>max.poll.interval.ms</code></li>
     * <li><code>max.poll.records</code></li>
     * <li><code>auto.commit.interval.ms</code></li>
     * </ol>
     * <p>
     * Though all of these configs have default values in kafka, we still require every user to set them specifically.
     * Because these configs is vital for using this consumer safely. You should tune them to ensure the polling thread
     * in this consumer can at least poll once within {@code max.poll.interval.ms} during handling consumed messages
     * to prevent itself from session timeout.
     * <p>
     * Note that if you set {@code enable.auto.commit} to false, this consumer will set it to true by itself.
     *
     * @return this
     */
    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAuto() {
        checkConfigs(AutoCommitConsumerConfigs.values());
        consumer = buildConsumer(true);
        policy = workerPool == ImmediateExecutorService.INSTANCE ? NoOpCommitPolicy.getInstance() : new AutoCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildSync() {
        consumer = buildConsumer(false);
        policy = new SyncCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialSync() {
        consumer = buildConsumer(false);
        policy = new PartialSyncCommitPolicy<>(consumer);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildAsync() {
        consumer = buildConsumer(false);
        policy = new AsyncCommitPolicy<>(consumer, maxPendingAsyncCommits);
        return doBuild();
    }

    public <K1 extends K, V1 extends V> LcKafkaConsumer<K1, V1> buildPartialAsync() {
        consumer = buildConsumer(false);
        policy = new PartialAsyncCommitPolicy<>(consumer, maxPendingAsyncCommits);
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

    long getPollTimeout() {
        return pollTimeout;
    }

    long gracefulShutdownMillis() {
        return gracefulShutdownMillis;
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
