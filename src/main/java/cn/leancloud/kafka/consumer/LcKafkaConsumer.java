package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;

/**
 * {@code LcKafkaConsumer} is a wrapper over {@link Consumer}. It will use {@link Consumer} to consume
 * records from Kafka broker.
 * <p>
 * With {@link LcKafkaConsumer}, you can subscribe to several topics and handle all the records from these topics
 * in a dedicated thread pool without warring polling timeout or session timeout due to the polling thread spend
 * too much time on process records and failed to poll broker at least once within {@code max.poll.interval.ms}.
 * <p>
 * All the public methods in {@code LcKafkaConsumer} is thread safe.
 *
 * @param <K> the type of key for records consumed from Kafka
 * @param <V> the type of value for records consumed from Kafka
 */
public final class LcKafkaConsumer<K, V> implements Closeable {
    enum State {
        INIT(0),
        SUBSCRIBED(1),
        CLOSED(2);

        private int code;

        State(int code) {
            this.code = code;
        }

        int code() {
            return code;
        }
    }

    private final Consumer<K, V> consumer;
    private final Thread fetcherThread;
    private final Fetcher<K, V> fetcher;
    private final ExecutorService workerPool;
    private final CommitPolicy<K, V> policy;
    private final boolean shutdownWorkerPoolOnStop;
    private volatile State state;

    LcKafkaConsumer(LcKafkaConsumerBuilder<K, V> builder) {
        this.state = State.INIT;
        this.consumer = builder.getConsumer();
        this.workerPool = builder.getWorkerPool();
        this.shutdownWorkerPoolOnStop = builder.isShutdownWorkerPoolOnStop();
        this.policy = builder.getPolicy();
        this.fetcher = new Fetcher<>(builder);
        this.fetcherThread = new Thread(fetcher);
    }

    /**
     * Subscribe some Kafka topics to consume records from them.
     *
     * @param topics the topics to consume.
     * @throws IllegalStateException    if this {@code LcKafkaConsumer} has closed or subscribed to some topics
     * @throws NullPointerException     if the input {@code topics} is null
     * @throws IllegalArgumentException if the input {@code topics} is empty or contains null or empty topic
     */
    public synchronized void subscribe(Collection<String> topics) {
        requireNonNull(topics, "topics");

        if (topics.isEmpty()) {
            throw new IllegalArgumentException("subscribe empty topics");
        }

        for (String topic : topics) {
            if (topic == null || topic.trim().isEmpty())
                throw new IllegalArgumentException("topic collection to subscribe to cannot contain null or empty topic");
        }

        ensureInInit();

        consumer.subscribe(topics, new RebalanceListener<>(consumer, policy));

        fetcherThread.setName(fetcherThreadName(topics));
        fetcherThread.start();

        state = State.SUBSCRIBED;
    }

    /**
     * Subscribe to all topics matching specified pattern to get dynamically assigned partitions.
     * The pattern matching will be done periodically against all topics existing at the time of check.
     * This can be controlled through the {@code metadata.max.age.ms} configuration: by lowering
     * the max metadata age, the consumer will refresh metadata more often and check for matching topics.
     *
     * @param pattern {@link Pattern} to subscribe to.
     * @throws IllegalStateException    if this {@code LcKafkaConsumer} has closed or subscribed to some topics
     * @throws NullPointerException     if the input {@code pattern} is null
     * @throws IllegalArgumentException if the input {@code pattern} is null
     */
    public synchronized void subscribe(Pattern pattern) {
        requireNonNull(pattern, "pattern");

        ensureInInit();

        consumer.subscribe(pattern, new RebalanceListener<>(consumer, policy));

        fetcherThread.setName(fetcherThreadName(pattern));
        fetcherThread.start();

        state = State.SUBSCRIBED;
    }

    /**
     * Get the metrics kept by the consumer
     *
     * @return the metrics kept by the consumer
     */
    public Map<MetricName, ? extends Metric> metrics() {
        return consumer.metrics();
    }

    @Override
    public void close() {
        if (closed()) {
            return;
        }

        synchronized (this) {
            if (closed()) {
                return;
            }

            state = State.CLOSED;
        }

        fetcher.close();
        try {
            fetcherThread.join();
            consumer.close();
            if (shutdownWorkerPoolOnStop) {
                workerPool.shutdown();
                workerPool.awaitTermination(1, TimeUnit.DAYS);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    boolean subscribed() {
        return state.code() > State.INIT.code();
    }

    boolean closed() {
        return state == State.CLOSED;
    }

    CommitPolicy<K, V> policy() {
        return policy;
    }

    private void ensureInInit() {
        if (subscribed() || closed()) {
            throw new IllegalStateException("client is in " + state + " state. expect: " + State.INIT);
        }
    }

    private String fetcherThreadName(Collection<String> topics) {
        final String firstTopic = topics.iterator().next();
        String postfix = firstTopic.substring(0, min(10, firstTopic.length()));
        postfix += topics.size() > 1 || firstTopic.length() > 10 ? "..." : "";
        return "kafka-fetcher-for-" + postfix;
    }

    private String fetcherThreadName(Pattern pattern) {
        final String patternInString = pattern.toString();
        String postfix = patternInString.substring(0, min(10, patternInString.length()));
        postfix += patternInString.length() > 10 ? "..." : "";
        return "kafka-fetcher-for-" + postfix;
    }
}
