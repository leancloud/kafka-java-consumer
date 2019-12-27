package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@code LcKafkaConsumer} is a wrapper over {@link Consumer}. It will use {@link Consumer} to consume
 * records from Kafka broker.
 * <p>
 * With {@link LcKafkaConsumer}, you can subscribe to several topics and handle all the records from these topic
 * in a dedicated thread pool without warring polling timeout or session timeout due to the polling thread failed
 * to poll spend too much time on process records
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
     * @throws IllegalArgumentException if the input {@code topics} is empty
     */
    public synchronized void subscribe(Collection<String> topics) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("subscribe empty topics");
        }

        if (subscribed() || closed()) {
            throw new IllegalStateException("client is in " + state + " state. expect: " + State.INIT);
        }

        consumer.subscribe(topics, new RebalanceListener<>(consumer, policy));

        final String firstTopic = topics.iterator().next();
        fetcherThread.setName("kafka-fetcher-for-" + firstTopic + (topics.size() > 1 ? "..." : ""));
        fetcherThread.start();

        state = State.SUBSCRIBED;
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
            // ignore
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
}
