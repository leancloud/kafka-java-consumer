package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

final class Fetcher<K, V> implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Fetcher.class);

    private final long pollTimeout;
    private final Consumer<K, V> consumer;
    private final MessageHandler<K, V> handler;
    private final ExecutorCompletionService<ConsumerRecord<K, V>> service;
    private final Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures;
    private final CommitPolicy<K, V> policy;
    private final long gracefulShutdownMillis;
    private volatile boolean closed;

    Fetcher(LcKafkaConsumerBuilder<K, V> consumerBuilder) {
        this(consumerBuilder.getConsumer(), consumerBuilder.getPollTimeout(), consumerBuilder.getMessageHandler(),
                consumerBuilder.getWorkerPool(), consumerBuilder.getPolicy(), consumerBuilder.gracefulShutdownMillis());
    }

    Fetcher(Consumer<K, V> consumer,
            long pollTimeout,
            MessageHandler<K, V> handler,
            ExecutorService workerPool,
            CommitPolicy<K, V> policy,
            long gracefulShutdownMillis) {
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
        this.pollTimeout = pollTimeout;
        this.handler = handler;
        this.service = new ExecutorCompletionService<>(workerPool);
        this.policy = policy;
        this.gracefulShutdownMillis = gracefulShutdownMillis;
    }

    @Override
    public void run() {
        logger.debug("Fetcher thread started.");
        final long pollTimeout = this.pollTimeout;
        final Consumer<K, V> consumer = this.consumer;
        while (true) {
            try {
                final ConsumerRecords<K, V> records = consumer.poll(pollTimeout);

                if (logger.isDebugEnabled()) {
                    logger.debug("Fetched " + records.count() + " records from: " + records.partitions());
                }

                dispatchFetchedRecords(records);
                processCompletedRecords();

                if (!pendingFutures.isEmpty() && !records.isEmpty()) {
                    consumer.pause(records.partitions());
                }

                tryCommitRecordOffsets();
            } catch (WakeupException ex) {
                if (closed()) {
                    break;
                }
            } catch (Exception ex) {
                close();
                logger.error("Fetcher quit with unexpected exception. Will rebalance after poll timeout.", ex);
                break;
            }
        }

        gracefulShutdown();
    }

    @Override
    public void close() {
        closed = true;
        consumer.wakeup();
    }

    Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures() {
        return pendingFutures;
    }

    private boolean closed() {
        return closed;
    }

    private void dispatchFetchedRecords(ConsumerRecords<K, V> records) {
        final MessageHandler<K, V> handler = this.handler;
        for (ConsumerRecord<K, V> record : records) {
            final Future<ConsumerRecord<K, V>> future = service.submit(() -> {
                handler.handleMessage(record);
                return record;
            });
            pendingFutures.put(record, future);
            policy.addPendingRecord(record);
        }
    }

    private void processCompletedRecords() throws InterruptedException, ExecutionException {
        Future<ConsumerRecord<K, V>> f;
        while ((f = service.poll()) != null) {
            assert f.isDone();
            final ConsumerRecord<K, V> r = f.get();
            final Future<ConsumerRecord<K, V>> v = pendingFutures.remove(r);
            assert v != null;
            policy.completeRecord(r);
        }
    }

    private void tryCommitRecordOffsets() {
        final Set<TopicPartition> partitions = policy.tryCommit(pendingFutures.isEmpty());
        if (!partitions.isEmpty()) {
            // `partitions` may have some revoked partitions so resume may throws IllegalStateException.
            // But rebalance is comparatively rare on production environment. So here we
            // try the optimised way first, if we got any IllegalStateException, we clean the revoked
            // partitions out of `partitions` and retry resume.
            try {
                consumer.resume(partitions);
            } catch (IllegalStateException ex) {
                partitions.retainAll(consumer.assignment());
                consumer.resume(partitions);
            }
        }
    }

    private void gracefulShutdown() {
        final long start = System.currentTimeMillis();
        long remain = gracefulShutdownMillis;
        try {
            for (Future<ConsumerRecord<K, V>> future : pendingFutures.values()) {
                try {
                    if (remain > 0) {
                        future.get(remain, TimeUnit.MILLISECONDS);
                        remain = gracefulShutdownMillis - (System.currentTimeMillis() - start);
                    } else {
                        future.cancel(false);
                    }
                } catch (TimeoutException ex) {
                    remain = 0;
                }
            }
            processCompletedRecords();
        } catch (InterruptedException ex) {
            logger.warn("Graceful shutdown was interrupted.");
        } catch (ExecutionException ex) {
            logger.error("Handle message got unexpected exception. Continue shutdown without wait handling message done.", ex);
        }

        policy.partialCommit();

        pendingFutures.clear();

        logger.debug("Fetcher thread exit.");
    }
}
