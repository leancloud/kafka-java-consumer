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
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


import static cn.leancloud.kafka.consumer.BasicConsumerConfigs.ENABLE_AUTO_COMMIT;
import static java.util.Objects.requireNonNull;

public final class LcKafkaConsumerBuilder<K, V> {
    private static final ThreadFactory threadFactory = new NamedThreadFactory("lc-kafka-consumer-task-worker-pool-");

    /**
     * Create a {@code LcKafkaConsumerBuilder} used to build {@link LcKafkaConsumer}.
     *
     * @param kafkaConfigs   the kafka consumer configs. Please refer
     *                       <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >this document</a> for
     *                       valid configurations.
     * @param messageHandler a {@link MessageHandler} to handle the consumed msg from kafka
     * @return a new {@code LcKafkaConsumerBuilder}
     */
    public static <K, V> LcKafkaConsumerBuilder<K, V> newBuilder(Map<String, Object> kafkaConfigs,
                                                                 MessageHandler<K, V> messageHandler) {
        requireNonNull(kafkaConfigs, "kafkaConfigs");
        requireNonNull(messageHandler, "messageHandler");
        return new LcKafkaConsumerBuilder<>(new HashMap<>(kafkaConfigs), messageHandler);
    }

    /**
     * Create a {@code LcKafkaConsumerBuilder} used to build {@link LcKafkaConsumer}.
     *
     * @param kafkaConfigs      the kafka consumer configs. Please refer
     *                          <a href="http://kafka.apache.org/documentation.html#consumerconfigs" >this document</a> for
     *                          valid configurations.
     * @param messageHandler    a {@link MessageHandler} to handle the consumed msg from kafka
     * @param keyDeserializer   The deserializer for key that implements {@link Deserializer}
     * @param valueDeserializer The deserializer for value that implements {@link Deserializer}
     * @return a new {@code LcKafkaConsumerBuilder}
     */
    public static <K, V> LcKafkaConsumerBuilder<K, V> newBuilder(Map<String, Object> kafkaConfigs,
                                                                 MessageHandler<K, V> messageHandler,
                                                                 Deserializer<K> keyDeserializer,
                                                                 Deserializer<V> valueDeserializer) {
        requireNonNull(kafkaConfigs, "kafkaConfigs");
        requireNonNull(messageHandler, "messageHandler");
        requireNonNull(keyDeserializer, "keyDeserializer");
        requireNonNull(valueDeserializer, "valueDeserializer");
        return new LcKafkaConsumerBuilder<>(new HashMap<>(kafkaConfigs), messageHandler, keyDeserializer, valueDeserializer);
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
    private Map<String, Object> configs;
    private MessageHandler<K, V> messageHandler;
    @Nullable
    private Consumer<K, V> consumer;
    @Nullable
    private Deserializer<K> keyDeserializer;
    @Nullable
    private Deserializer<V> valueDeserializer;
    @Nullable
    private CommitPolicy<K, V> policy;
    @Nullable
    private ExecutorService workerPool;
    private boolean shutdownWorkerPoolOnStop;

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   MessageHandler<K, V> messageHandler) {
        this(kafkaConsumerConfigs, messageHandler, null, null);
    }

    private LcKafkaConsumerBuilder(Map<String, Object> kafkaConsumerConfigs,
                                   MessageHandler<K, V> messageHandler,
                                   @Nullable
                                   Deserializer<K> keyDeserializer,
                                   @Nullable
                                   Deserializer<V> valueDeserializer) {
        this.configs = kafkaConsumerConfigs;
        this.messageHandler = messageHandler;
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
    public LcKafkaConsumerBuilder<K, V> pollTimeoutMs(long pollTimeoutMs) {
        requireArgument(pollTimeoutMs >= 0, "pollTimeoutMs: %s (expect >= 0)", pollTimeoutMs);
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

    public LcKafkaConsumerBuilder<K,V> gracefulShutdownMs(long gracefulShutdownMs) {
        requireArgument(gracefulShutdownMs >= 0, "gracefulShutdownMillis: %s (expected >= 0)", gracefulShutdownMs);
        this.gracefulShutdownMillis = gracefulShutdownMs;
        return this;
    }

    public LcKafkaConsumerBuilder<K,V> gracefulShutdownMs(Duration duration) {
        requireNonNull(duration);
        this.gracefulShutdownMillis = duration.toMillis();
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
     * Change the {@link MessageHandler} to handle the consumed msg from kafka.
     *
     * @param messageHandler the handler to handle consumed msg
     * @return this
     */
    public LcKafkaConsumerBuilder<K, V> messageHandler(MessageHandler<K, V> messageHandler) {
        requireNonNull(messageHandler, "messageHandler");
        this.messageHandler = messageHandler;
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
     *         handler.handleMessage(record.topic(), record.value());
     *     }
     * }
     * </pre>
     *
     * <p>
     * Please note that this consumer requires these kafka configs must be set, otherwise
     * {@link IllegalArgumentException} will be thrown:
     * <ol>
     *  <li><code>max.poll.interval.ms</code></li>
     *  <li><code>max.poll.records</code></li>
     *  <li><code>auto.commit.interval.ms</code></li>
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
        policy = AutoCommitPolicy.getInstance();
        if (workerPool != null && shutdownWorkerPoolOnStop) {
            throw new IllegalArgumentException("auto commit consumer don't need a worker pool");
        }
        workerPool = ImmediateExecutorService.INSTANCE;
        shutdownWorkerPoolOnStop = false;
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

    MessageHandler<K, V> getMessageHandler() {
        return messageHandler;
    }

    ExecutorService getWorkerPool() {
        assert workerPool != null;
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
        if (workerPool == null) {
            workerPool = Executors.newCachedThreadPool(threadFactory);
            shutdownWorkerPoolOnStop = true;
        }

        @SuppressWarnings("unchecked")
        final LcKafkaConsumer<K1, V1> c = (LcKafkaConsumer<K1, V1>) new LcKafkaConsumer<>(this);
        return c;
    }
}
