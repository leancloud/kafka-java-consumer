package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
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
    private static final long pollTimeout = 100;
    private MockConsumer<Object, Object> consumer;
    private ConsumerRecordHandler<Object, Object> consumerRecordHandler;
    private CommitPolicy<Object, Object> policy;
    private ExecutorService executorService;
    private Fetcher<Object, Object> fetcher;
    private Thread fetcherThread;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumerRecordHandler = mock(ConsumerRecordHandler.class);
        policy = mock(CommitPolicy.class);
        executorService = ImmediateExecutorService.INSTANCE;
    }

    @After
    public void tearDown() throws Exception {
        fetcher.close();
        fetcherThread.join();
        consumer.close();
        executorService.shutdown();
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        executorService = mock(ExecutorService.class);
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, 0);
        fetcherThread = new Thread(fetcher);

        doNothing().when(executorService).execute(any(Runnable.class));
        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        consumer.addRecord(defaultTestingRecord);
        fetcherThread.start();

        await().until(() -> !fetcher.pendingFutures().isEmpty());
        assertThat(fetcher.pendingFutures()).hasSize(1).containsOnlyKeys(defaultTestingRecord);
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommit();
    }

    @Test
    public void testHandleMsgFailed() throws Exception {
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, 10_000);
        fetcherThread = new Thread(fetcher);

        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        consumer.addRecord(defaultTestingRecord);
        doThrow(new RuntimeException("expected exception")).when(consumerRecordHandler).handleRecord(defaultTestingRecord);

        fetcherThread.start();

        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        verify(policy, times(1)).partialCommit();
        verify(policy, times(1)).addPendingRecord(eq(defaultTestingRecord));
        verify(policy, never()).completeRecord(any());
        verify(policy, never()).tryCommit(anyBoolean());
        verify(consumerRecordHandler, times(1)).handleRecord(defaultTestingRecord);
    }

    @Test
    public void testNoPauseWhenMsgHandledFastEnough() throws Exception {
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executorService, policy, 10_000);
        fetcherThread = new Thread(fetcher);

        final CyclicBarrier barrier = new CyclicBarrier(2);
        assignPartitions(consumer, toPartitions(singletonList(0)), 0L);
        when(policy.tryCommit(false)).thenReturn(Collections.emptySet());
        doAnswer(invocation -> {
            barrier.await();
            return null;
        }).when(policy).completeRecord(defaultTestingRecord);
        consumer.addRecord(defaultTestingRecord);
        fetcherThread.start();

        barrier.await();
        fetcher.close();
        fetcherThread.join();
        assertThat(fetcher.pendingFutures()).isEmpty();
        assertThat(consumer.paused()).isEmpty();
        verify(policy, times(1)).partialCommit();
        verify(policy, times(1)).addPendingRecord(eq(defaultTestingRecord));
        verify(policy, times(1)).completeRecord(defaultTestingRecord);
        verify(policy, atLeastOnce()).tryCommit(anyBoolean());
        verify(consumerRecordHandler, times(1)).handleRecord(defaultTestingRecord);
    }

    @Test
    public void testPauseResume() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors, policy, 10_000);
        fetcherThread = new Thread(fetcher);

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
        }).when(policy).completeRecord(any());

        // resume completed partitions
        when(policy.tryCommit(true)).thenReturn(completePartitions);

        fireConsumerRecords(consumer, pendingRecords);

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

        verify(policy, times(pendingRecords.size())).addPendingRecord(any());
        verify(policy, times(pendingRecords.size())).completeRecord(any());
        verify(policy, times(1)).partialCommit();
        verify(consumerRecordHandler, times(pendingRecords.size())).handleRecord(any());

        executors.shutdown();
    }

    @Test
    public void testPauseAndPartialResume() throws Exception {
        final ExecutorService executors = Executors.newCachedThreadPool();
        fetcher = new Fetcher<>(consumer, pollTimeout, consumerRecordHandler, executors, policy, 0);
        fetcherThread = new Thread(fetcher);

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
        }).when(policy).completeRecord(any());

        // resume completed partitions
        when(policy.tryCommit(false)).thenReturn(completePartitions);

        fireConsumerRecords(consumer, pendingRecords);

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

        verify(policy, times(pendingRecords.size())).addPendingRecord(any());
        verify(policy, times(pendingRecords.size() / 2)).completeRecord(any());
        verify(policy, times(1)).partialCommit();
        verify(consumerRecordHandler, times(pendingRecords.size())).handleRecord(any());

        executors.shutdown();
    }
}