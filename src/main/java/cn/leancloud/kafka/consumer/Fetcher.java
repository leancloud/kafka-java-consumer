package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

final class Fetcher<K, V> implements Runnable, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Fetcher.class);

    @VisibleForTesting
    static class TimeoutFuture<K, V> implements Future<ConsumerRecord<K, V>> {
        private final Future<ConsumerRecord<K, V>> wrappedFuture;
        private final long timeoutAtNanos;
        private final Time time;

        TimeoutFuture(Future<ConsumerRecord<K, V>> wrappedFuture, long timeoutInNanos) {
            this(wrappedFuture, timeoutInNanos, Time.SYSTEM);
        }

        TimeoutFuture(Future<ConsumerRecord<K, V>> wrappedFuture, long timeoutInNanos, Time time) {
            assert timeoutInNanos >= 0;
            this.wrappedFuture = wrappedFuture;
            long timeoutAtNanos = time.nanoseconds() + timeoutInNanos;
            if (timeoutAtNanos < 0) {
                timeoutAtNanos = Long.MAX_VALUE;
            }
            this.timeoutAtNanos = timeoutAtNanos;
            this.time = time;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return wrappedFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return wrappedFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return wrappedFuture.isDone();
        }

        @Override
        public ConsumerRecord<K, V> get() throws InterruptedException, ExecutionException {
            return wrappedFuture.get();
        }

        @Override
        public ConsumerRecord<K, V> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            // if it is already timeout, throw exception immediately
            if (timeout()) {
                throw new TimeoutException();
            }

            final long timeoutNanos = Math.max(0, Math.min(unit.toNanos(timeout), timeoutAtNanos - time.nanoseconds()));
            return wrappedFuture.get(timeoutNanos, TimeUnit.NANOSECONDS);
        }

        boolean timeout() {
            return time.nanoseconds() >= timeoutAtNanos;
        }
    }

    private final long pollTimeoutMillis;
    private final Consumer<K, V> consumer;
    private final ConsumerRecordHandler<K, V> handler;
    private final ExecutorCompletionService<ConsumerRecord<K, V>> service;
    private final Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures;
    private final CommitPolicy policy;
    private final long gracefulShutdownTimeoutNanos;
    private final CompletableFuture<UnsubscribedStatus> unsubscribeStatusFuture;
    private final long handleRecordTimeoutNanos;
    private final ProcessRecordsProgress progress;
    private volatile boolean closed;

    Fetcher(LcKafkaConsumerBuilder<K, V> consumerBuilder) {
        this(consumerBuilder.getConsumer(), consumerBuilder.getPollTimeout(), consumerBuilder.getConsumerRecordHandler(),
                consumerBuilder.getWorkerPool(), consumerBuilder.getPolicy(), consumerBuilder.getGracefulShutdownTimeout(),
                consumerBuilder.getHandleRecordTimeout(), new ProcessRecordsProgress());
    }

    Fetcher(Consumer<K, V> consumer,
            Duration pollTimeout,
            ConsumerRecordHandler<K, V> handler,
            ExecutorService workerPool,
            CommitPolicy policy,
            Duration gracefulShutdownTimeout,
            Duration handleRecordTimeout,
            ProcessRecordsProgress progress) {
        this.progress = progress;
        this.pendingFutures = new HashMap<>();
        this.consumer = consumer;
        this.pollTimeoutMillis = pollTimeout.toMillis();
        this.handler = handler;
        this.service = new ExecutorCompletionService<>(workerPool);
        this.policy = policy;
        this.gracefulShutdownTimeoutNanos = gracefulShutdownTimeout.toNanos();
        this.unsubscribeStatusFuture = new CompletableFuture<>();
        this.handleRecordTimeoutNanos = handleRecordTimeout.toNanos();
    }

    @Override
    public void run() {
        logger.debug("Fetcher thread started.");
        final long pollTimeoutMillis = this.pollTimeoutMillis;
        final Consumer<K, V> consumer = this.consumer;
        UnsubscribedStatus unsubscribedStatus = UnsubscribedStatus.CLOSED;
        while (true) {
            try {
                final ConsumerRecords<K, V> records = consumer.poll(pollTimeoutMillis);

                if (logger.isDebugEnabled()) {
                    logger.debug("Fetched " + records.count() + " records from: " + records.partitions());
                }

                dispatchFetchedRecords(records);
                processCompletedRecords();
                processTimeoutRecords();

                if (!pendingFutures.isEmpty() && !records.isEmpty()) {
                    consumer.pause(records.partitions());
                }

                tryCommitRecordOffsets();
            } catch (WakeupException ex) {
                if (closed()) {
                    break;
                }
            } catch (ExecutionException ex) {
                unsubscribedStatus = UnsubscribedStatus.ERROR;
                markClosed();
                break;
            } catch (Throwable ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }

                unsubscribedStatus = UnsubscribedStatus.ERROR;
                markClosed();
                logger.error("Fetcher quit with unexpected exception.", ex);
                break;
            }
        }

        gracefulShutdown(unsubscribedStatus);
        logger.debug("Fetcher thread exit.");
    }

    @Override
    public void close() {
        markClosed();
        consumer.wakeup();
    }

    CompletableFuture<UnsubscribedStatus> unsubscribeStatusFuture() {
        return unsubscribeStatusFuture;
    }

    ProcessRecordsProgress progress() {
        return progress;
    }

    @VisibleForTesting
    Map<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> pendingFutures() {
        return pendingFutures;
    }

    private void markClosed() {
        closed = true;
    }

    private boolean closed() {
        return closed;
    }

    private void dispatchFetchedRecords(ConsumerRecords<K, V> records) {
        final ConsumerRecordHandler<K, V> handler = this.handler;
        for (ConsumerRecord<K, V> record : records) {
            final Future<ConsumerRecord<K, V>> future = service.submit(() -> {
                handler.handleRecord(record);
                return record;
            });
            pendingFutures.put(record, timeoutAwareFuture(future));
            progress.markPendingRecord(record);
        }
    }

    private Future<ConsumerRecord<K, V>> timeoutAwareFuture(Future<ConsumerRecord<K, V>> future) {
        if (unlimitedHandleRecordTime()) {
            return future;
        } else {
            return new TimeoutFuture<>(future, handleRecordTimeoutNanos);
        }
    }

    private void processCompletedRecords() throws InterruptedException, ExecutionException {
        Future<ConsumerRecord<K, V>> f;
        while ((f = service.poll()) != null) {
            processCompletedRecord(f);
        }
    }

    private void processCompletedRecord(Future<ConsumerRecord<K, V>> future) throws InterruptedException, ExecutionException {
        assert future.isDone();
        final ConsumerRecord<K, V> record = future.get();
        assert record != null;
        assert !future.isCancelled();
        final Future<ConsumerRecord<K, V>> v = pendingFutures.remove(record);
        assert v != null;
        progress.markCompletedRecord(record);
    }

    private void processTimeoutRecords() throws TimeoutException {
        if (unlimitedHandleRecordTime()) {
            return;
        }

        for (Map.Entry<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> entry : pendingFutures.entrySet()) {
            // we can sure that this conversion must be success
            final TimeoutFuture<K, V> future = (TimeoutFuture<K, V>) entry.getValue();
            if (future.timeout()) {
                future.cancel(false);
                // do not wait for it again on graceful shutdown
                pendingFutures.remove(entry.getKey(), entry.getValue());
                throw new TimeoutException("timeout on handling record: " + entry.getKey());
            }
        }
    }

    private void tryCommitRecordOffsets() {
        final Set<TopicPartition> partitions = policy.tryCommit(pendingFutures.isEmpty(), progress);
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

    private boolean unlimitedHandleRecordTime() {
        return handleRecordTimeoutNanos == 0;
    }

    private void gracefulShutdown(UnsubscribedStatus unsubscribedStatus) {
        long shutdownTimeout = 0L;
        try {
            shutdownTimeout = waitPendingFuturesDone();
            policy.partialCommitSync(progress);
            pendingFutures.clear();
        } catch (Exception ex) {
            logger.error("Graceful shutdown got unexpected exception", ex);
        } finally {
            try {
                consumer.close(shutdownTimeout, TimeUnit.NANOSECONDS);
            } finally {
                unsubscribeStatusFuture.complete(unsubscribedStatus);
            }
        }
    }

    private long waitPendingFuturesDone() {
        final long start = System.nanoTime();
        long remain = gracefulShutdownTimeoutNanos;

        for (Map.Entry<ConsumerRecord<K, V>, Future<ConsumerRecord<K, V>>> entry : pendingFutures.entrySet()) {
            final Future<ConsumerRecord<K, V>> future = entry.getValue();
            try {
                assert remain >= 0;
                final ConsumerRecord<K, V> record = future.get(remain, TimeUnit.MILLISECONDS);
                assert record != null;
                progress.markCompletedRecord(record);
            } catch (TimeoutException ex) {
                future.cancel(false);
            } catch (InterruptedException ex) {
                future.cancel(false);
                Thread.currentThread().interrupt();
            } catch (CancellationException ex) {
                // ignore
            } catch (ExecutionException ex) {
                logger.error("Fetcher quit with unexpected exception on handling consumer record: " + entry.getKey(), ex.getCause());
            } finally {
                if (remain >= 0) {
                    remain = Math.max(0, gracefulShutdownTimeoutNanos - (System.nanoTime() - start));
                }
            }
        }

        return remain;
    }
}
