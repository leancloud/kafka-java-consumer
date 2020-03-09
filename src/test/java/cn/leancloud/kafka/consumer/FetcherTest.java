package cn.leancloud.kafka.consumer;

import cn.leancloud.kafka.consumer.Fetcher.TimeoutFuture;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.AdditionalMatchers;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

public class FetcherTest {
    private static final String testingTopic = "TestingTopic";
    private static final Object defaultKey = new Object();
    private static final Object defaultMsg = new Object();
    private static final ConsumerRecord<Object, Object> defaultTestingRecord =
            new ConsumerRecord<>(
                    testingTopic,
                    0,
                    1, defaultKey,
                    defaultMsg);
    private static final Duration pollTimeout = Duration.ofMillis(100);
    private static final Duration defaultGracefulShutdownTimeout = Duration.ofSeconds(10);
    private MockConsumer<Object, Object> consumer;
    private ConsumerRecordHandler<Object, Object> consumerRecordHandler;
    private CommitPolicy policy;
    private ExecutorService executorService;
    private ProcessRecordsProgress progress;
    @Nullable
    private Fetcher<Object, Object> fetcher;
    @Nullable
    private Thread fetcherThread;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumerRecordHandler = mock(ConsumerRecordHandler.class);
        policy = mock(CommitPolicy.class);
        progress = mock(ProcessRecordsProgress.class);
        executorService = ImmediateExecutorService.INSTANCE;
    }

    @After
    public void tearDown() throws Exception {
        if (fetcher != null) {
            fetcher.close();
        }
        if (fetcherThread != null) {
            fetcherThread.join();
        }
        if (!consumer.closed()) {
            consumer.close();
        }
        executorService.shutdown();
    }

    @Test
    public void testDelegateMethodInTimeoutFuture() throws Exception {
        final Future<ConsumerRecord<Object, Object>> wrappedFuture = mock(Future.class);
        when(wrappedFuture.get()).thenReturn(null);

        TimeoutFuture<Object, Object> future = new TimeoutFuture<>(wrappedFuture, 100);

        future.cancel(false);
        future.cancel(true);
        future.isCancelled();
        future.isDone();
        future.get();

        verify(wrappedFuture, times(1)).cancel(false);
        verify(wrappedFuture, times(1)).cancel(true);
        verify(wrappedFuture, times(1)).isCancelled();
        verify(wrappedFuture, times(1)).isDone();
        verify(wrappedFuture, times(1)).get();
    }

    @Test
    public void testTimeoutOnGetTimeoutFuture() {
        final Future<ConsumerRecord<Object, Object>> wrappedFuture = mock(Future.class);
        final TimeoutFuture<Object, Object> future = new TimeoutFuture<>(wrappedFuture, 0);
        assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    public void testTimeoutOverflowOnTimeoutFuture() {
        final MockTime time = new MockTime();
        final Future<ConsumerRecord<Object, Object>> wrappedFuture = mock(Future.class);
        final TimeoutFuture<Object, Object> future = new TimeoutFuture<>(wrappedFuture, Long.MAX_VALUE - 1, time);
        time.setCurrentTimeNanos(Long.MAX_VALUE - 1);
        assertThat(future.timeout()).isFalse();
        time.setCurrentTimeNanos(Long.MAX_VALUE);
        assertThat(future.timeout()).isTrue();
    }

    @Test
    public void testGetOnTimeoutFuture() throws Exception {
        final Future<ConsumerRecord<Object, Object>> wrappedFuture = mock(Future.class);
        when(wrappedFuture.get(10L, TimeUnit.SECONDS)).thenReturn(null);

        final TimeoutFuture<Object, Object> future = new TimeoutFuture<>(wrappedFuture, TimeUnit.SECONDS.toNanos(20));
        future.get(10, TimeUnit.SECONDS);
        verify(wrappedFuture, times(1)).get(TimeUnit.SECONDS.toNanos(10), TimeUnit.NANOSECONDS);
    }

    @Test
    public void testGetOnTimeoutFuture2() throws Exception {
        final Future<ConsumerRecord<Object, Object>> wrappedFuture = mock(Future.class);
        when(wrappedFuture.get(10L, TimeUnit.SECONDS)).thenReturn(null);
        final TimeoutFuture<Object, Object> future = new TimeoutFuture<>(wrappedFuture, TimeUnit.SECONDS.toNanos(5));
        future.get(10, TimeUnit.SECONDS);
        verify(wrappedFuture, times(1))
                .get(AdditionalMatchers.lt(TimeUnit.SECONDS.toNanos(5)), any());
    }

    @Test
    public void testUnexpectedException() throws Exception {
        executorService = mock(ExecutorService.class);
        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        consumer.setException(new KafkaException("expected testing exception"));
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, defaultGracefulShutdownTimeout,
                Duration.ofSeconds(10), progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);
        fetcherThread.start();

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommitSync(progress);
        assertThat(consumer.subscription()).isEmpty();
        assertThat(consumer.closed()).isTrue();
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.ERROR);
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        executorService = mock(ExecutorService.class);
        doNothing().when(executorService).execute(any(Runnable.class));
        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        consumer.addRecord(defaultTestingRecord);

        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, Duration.ZERO, Duration.ZERO, progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        await().until(() -> !fetcher.pendingFutures().isEmpty());
        assertThat(fetcher.pendingFutures()).hasSize(1).containsOnlyKeys(defaultTestingRecord);
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommitSync(progress);
        assertThat(consumer.subscription()).isEmpty();
        assertThat(consumer.closed()).isTrue();
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.CLOSED);
    }

    @Test
    public void testInterruptOnShutdown() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        final CountDownLatch latch = new CountDownLatch(1);
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0);
        fireConsumerRecords(consumer, pendingRecords);
        doThrow(new RuntimeException("expected exception"))
                .doAnswer(invocation -> {
                    latch.countDown();
                    Thread.sleep(5000);
                    return null;
                })
                .when(consumerRecordHandler).handleRecord(any());
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors, policy, defaultGracefulShutdownTimeout, Duration.ZERO, progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        latch.await();

        fetcherThread.interrupt();

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommitSync(progress);
        verify(progress, never()).markCompletedRecord(any());
        verify(policy, never()).tryCommit(true, progress);
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.ERROR);
        executors.shutdownNow();
    }

    @Test
    public void testExceptionThrownOnShutdown() throws Exception {
        executorService = mock(ExecutorService.class);
        when(policy.partialCommitSync(any())).thenThrow(new RuntimeException("expected testing exception"));
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, Duration.ZERO, Duration.ZERO, progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        fetcher.close();
        fetcherThread.join();
        verify(policy, times(1)).partialCommitSync(progress);
        assertThat(consumer.subscription()).isEmpty();
        assertThat(consumer.closed()).isTrue();
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.CLOSED);
    }

    @Test
    public void testCloseConsumerFailed() throws Exception {
        executorService = mock(ExecutorService.class);
        Consumer<Object, Object> mockedConsumer = mock(MockConsumer.class);
        when(mockedConsumer.poll(anyLong())).thenThrow(new WakeupException());
        doThrow(new RuntimeException("expected testing exception")).when(mockedConsumer).close();
        fetcher = new Fetcher<>(mockedConsumer, pollTimeout, consumerRecordHandler, executorService, policy,
                Duration.ofNanos(100), Duration.ZERO, progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        fetcher.close();
        fetcherThread.join();
        verify(policy, times(1)).partialCommitSync(progress);
        verify(mockedConsumer, times(1)).close(100, TimeUnit.NANOSECONDS);
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.CLOSED);
    }

    @Test
    public void testHandleMsgFailed() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.addRecord(defaultTestingRecord);
        doThrow(new RuntimeException("expected exception"))
                .doNothing()
                .when(consumerRecordHandler).handleRecord(any());

        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors, policy, defaultGracefulShutdownTimeout, Duration.ZERO, progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommitSync(progress);
        verify(progress, times(pendingRecords.size())).markPendingRecord(any());
        verify(progress, times(pendingRecords.size() - 1)).markCompletedRecord(any());
        verify(policy, never()).tryCommit(true, progress);
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.ERROR);
        assertThat(consumer.closed()).isTrue();
        executors.shutdown();
    }

    @Test
    public void testHandleMsgTimeout() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        consumer.addRecord(defaultTestingRecord);
        doAnswer(invocation -> {
            Thread.sleep(1000);
            return null;
        }).when(consumerRecordHandler).handleRecord(defaultTestingRecord);
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors,
                policy, defaultGracefulShutdownTimeout, Duration.ofMillis(100), progress);
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = fetcher.unsubscribeStatusFuture();
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommitSync(progress);
        verify(progress, times(1)).markPendingRecord(eq(defaultTestingRecord));
        verify(progress, never()).markCompletedRecord(any());
        verify(policy, never()).tryCommit(true, progress);
        verify(consumerRecordHandler, times(1)).handleRecord(defaultTestingRecord);
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.ERROR);
        assertThat(consumer.closed()).isTrue();
        executors.shutdown();
    }

    @Test
    public void testNoPauseWhenMsgHandledFastEnough() throws Exception {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        when(policy.tryCommit(false, progress)).thenReturn(Collections.emptySet());
        doAnswer(invocation -> {
            barrier.await();
            return null;
        }).when(progress).markCompletedRecord(defaultTestingRecord);
        consumer.addRecord(defaultTestingRecord);

        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, defaultGracefulShutdownTimeout, Duration.ZERO, progress);
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        barrier.await();
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        assertThat(consumer.paused()).isEmpty();
        verify(policy, times(1)).partialCommitSync(progress);
        verify(progress, times(1)).markPendingRecord(eq(defaultTestingRecord));
        verify(progress, times(1)).markCompletedRecord(defaultTestingRecord);
        verify(policy, atLeastOnce()).tryCommit(anyBoolean(), any());
        verify(consumerRecordHandler, times(1)).handleRecord(defaultTestingRecord);
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    public void testPauseResume() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        final CyclicBarrier barrier = new CyclicBarrier(pendingRecords.size() + 1);
        final Set<TopicPartition> completePartitions = new HashSet<>();
        assignPartitions(consumer, partitions, 0L);

        doAnswer(invocation -> {
            // wait until the main thread figure out that all the partitions was paused
            barrier.await();
            return null;
        }).when(consumerRecordHandler).handleRecord(any());

        // record complete partitions
        doAnswer(invocation -> {
            final ConsumerRecord<Object, Object> completeRecord = invocation.getArgument(0);
            completePartitions.add(new TopicPartition(testingTopic, completeRecord.partition()));
            return null;
        }).when(progress).markCompletedRecord(any());

        // resume completed partitions
        when(policy.tryCommit(true, progress)).thenReturn(completePartitions);

        fireConsumerRecords(consumer, pendingRecords);

        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors, policy, defaultGracefulShutdownTimeout, Duration.ZERO, progress);
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        await().until(() -> consumer.paused().size() == pendingRecords.size());
        assertThat(consumer.paused()).isEqualTo(new HashSet<>(partitions));
        // release all the record handler threads
        barrier.await();
        // after the record was handled, the paused partition will be resumed eventually
        await().until(() -> consumer.paused().isEmpty());

        // close and verify
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();

        verify(progress, times(pendingRecords.size())).markPendingRecord(any());
        verify(progress, times(pendingRecords.size())).markCompletedRecord(any());
        verify(policy, times(1)).partialCommitSync(progress);
        verify(consumerRecordHandler, times(pendingRecords.size())).handleRecord(any());
        assertThat(consumer.closed()).isTrue();
        executors.shutdown();
    }

    @Test
    public void testPauseAndPartialResume() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        final CyclicBarrier barrier = new CyclicBarrier(pendingRecords.size() + 1);
        final Set<TopicPartition> completePartitions = new HashSet<>();
        assignPartitions(consumer, partitions, 0L);

        doAnswer(invocation -> {
            // wait until the main thread figure out that all the partitions was paused
            barrier.await();
            // only let half of the testing records handled
            final ConsumerRecord<Object, Object> record = invocation.getArgument(0);
            if (record.partition() < pendingRecords.size() / 2) {
                barrier.await();
            }
            return null;
        }).when(consumerRecordHandler).handleRecord(any());

        // record complete partitions
        doAnswer(invocation -> {
            final ConsumerRecord<Object, Object> completeRecord = invocation.getArgument(0);
            completePartitions.add(new TopicPartition(testingTopic, completeRecord.partition()));
            return null;
        }).when(progress).markCompletedRecord(any());

        // resume completed partitions
        when(policy.tryCommit(false, progress)).thenReturn(completePartitions);

        fireConsumerRecords(consumer, pendingRecords);

        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors, policy, Duration.ZERO, Duration.ZERO, progress);
        fetcherThread = new Thread(fetcher);

        fetcherThread.start();

        await().until(() -> consumer.paused().size() == pendingRecords.size());
        assertThat(consumer.paused()).isEqualTo(new HashSet<>(partitions));
        // release all the record handler threads
        barrier.await();
        // half of the records will be handled and their corresponding partitions will be resumed
        await().until(() -> consumer.paused().size() == pendingRecords.size() / 2);
        assertThat(fetcher.pendingFutures()).hasSize(pendingRecords.size() / 2);
        assertThat(fetcher.pendingFutures().keySet()).containsExactlyInAnyOrderElementsOf(pendingRecords.subList(0, pendingRecords.size() / 2));
        // close and verify
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();

        verify(progress, times(pendingRecords.size())).markPendingRecord(any());
        verify(progress, times(pendingRecords.size() / 2)).markCompletedRecord(any());
        verify(policy, times(1)).partialCommitSync(progress);
        verify(consumerRecordHandler, times(pendingRecords.size())).handleRecord(any());
        assertThat(consumer.closed()).isTrue();

        executors.shutdown();
    }
}