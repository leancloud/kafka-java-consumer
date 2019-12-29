package cn.leancloud.kafka.consumer.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class TestingProducer implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TestingProducer.class);

    private Duration sendInterval;
    private int concurrentThreadCount;
    private Producer<Integer, String> producer;
    private CyclicBarrier barrier;
    private List<Thread> workerThreads;
    private volatile boolean closed;

    TestingProducer(Duration sendInterval, int concurrentThreadCount) {
        this.sendInterval = sendInterval;
        this.concurrentThreadCount = concurrentThreadCount;
        this.workerThreads = new ArrayList<>();

        final Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        this.producer = new KafkaProducer<>(configs, new IntegerSerializer(), new StringSerializer());
        this.barrier = new CyclicBarrier(concurrentThreadCount + 1);
    }

    @Override
    public void close() {
        closed = true;
        try {
            for (Thread t : workerThreads) {
                t.join();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    CompletableFuture<Integer> startFixedDurationTest(String topic, Duration testingTime) throws Exception {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        final AtomicInteger finishedWorkerCount = new AtomicInteger();
        final AtomicInteger totalSentCount = new AtomicInteger();
        final Instant end = Instant.now().plus(testingTime);
        for (int i = 0; i < concurrentThreadCount; ++i) {
            final ProducerWorker worker = new ProducerWorker(topic, p -> Instant.now().isAfter(end));
            final Thread t = new Thread(worker);
            workerThreads.add(t);
            worker.future.thenApply(c -> {
                totalSentCount.addAndGet(c);
                if (finishedWorkerCount.incrementAndGet() == concurrentThreadCount) {
                    future.complete(totalSentCount.get());
                }

                return c;
            });
            t.start();
        }

        barrier.await();
        return future;
    }

    CompletableFuture<Integer> startNonStopTest(String topic, AtomicBoolean stopMark) throws Exception {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
        final AtomicInteger finishedWorkerCount = new AtomicInteger();
        final AtomicInteger totalSentCount = new AtomicInteger();
        for (int i = 0; i < concurrentThreadCount; ++i) {
            final ProducerWorker worker = new ProducerWorker(topic, p -> stopMark.get());
            final Thread t = new Thread(worker);
            workerThreads.add(t);
            worker.future.thenApply(c -> {
                totalSentCount.addAndGet(c);
                if (finishedWorkerCount.incrementAndGet() == concurrentThreadCount) {
                    future.complete(totalSentCount.get());
                }
                return c;
            });
            t.start();
        }

        barrier.await();
        return future;
    }

    private class ProducerWorker implements Runnable {
        private final Predicate<ProducerWorker> needStop;
        private final String topic;
        private final CompletableFuture<Integer> future;

        ProducerWorker(String topic, Predicate<ProducerWorker> needStop) {
            this.topic = topic;
            this.needStop = needStop;
            this.future = new CompletableFuture<>();
        }

        @Override
        public void run() {
            try {
                int sentCount = 0;
                final String topic = this.topic;
                final String name = Thread.currentThread().getName();
                final Predicate<ProducerWorker> needStop = this.needStop;
                final Producer<Integer, String> producer = TestingProducer.this.producer;
                final long intervalMs = TestingProducer.this.sendInterval.toMillis();
                barrier.await();
                logger.info("Producer worker: {} started", name);
                int index = 0;
                while (!closed && !needStop.test(this)) {
                    final ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, index, name + "-" + index++);
                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            logger.error("Produce record failed", exception);
                            closed = true;
                        }
                    });
                    ++sentCount;
                    Thread.sleep(intervalMs);
                }

                future.complete(sentCount);
                logger.info("Producer worker: {} stopped, sent: {} records", name, sentCount);
            } catch (Exception ex) {
                logger.error("Producer worker got unexpected exception", ex);
            }
        }
    }
}
