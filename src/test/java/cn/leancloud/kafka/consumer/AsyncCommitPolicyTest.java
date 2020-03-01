package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AsyncCommitPolicyTest {
    private static final int defaultMaxPendingAsyncCommits = 10;
    private MockConsumer<Object, Object> consumer;
    private AsyncCommitPolicy<Object, Object> policy;
    private List<TopicPartition> partitions;
    private List<ConsumerRecord<Object, Object>> pendingRecords;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new AsyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofHours(1), defaultMaxPendingAsyncCommits);
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        pendingRecords = generateConsumedRecords(consumer, partitions);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testHavePendingRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.completedTopicOffsets()).isNotEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isNotEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testNoCompleteRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        assignPartitions(consumer, partitions, 0L);
        generateConsumedRecords(consumer, partitions);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testSyncRecommit() throws Exception {
        policy = new AsyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofMillis(200), defaultMaxPendingAsyncCommits);
        long nextRecommitNanos = policy.nextRecommitNanos();
        assignPartitions(consumer, toPartitions(range(0, 30).boxed().collect(toList())), 0L);

        final List<ConsumerRecord<Object, Object>> prevRecords = generateConsumedRecords(consumer, toPartitions(range(0, 10).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> previousCommitOffsets = buildCommitOffsets(prevRecords);
        addCompleteRecordsInPolicy(policy, prevRecords);
        assertThat(policy.tryCommit(true))
                .containsExactlyInAnyOrderElementsOf(toPartitions(range(0, 10).boxed().collect(toList())));
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);

        Thread.sleep(200);
        nextRecommitNanos = policy.nextRecommitNanos();
        final List<ConsumerRecord<Object, Object>> newRecords = generateConsumedRecords(consumer, toPartitions(range(10, 20).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> newCommitOffsets = buildCommitOffsets(newRecords);
        newCommitOffsets.putAll(previousCommitOffsets);

        addCompleteRecordsInPolicy(policy, newRecords);
        policy.setForceSync(true);
        assertThat(policy.tryCommit(false)).isEmpty();
        assertThat(policy.forceSync()).isFalse();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : newCommitOffsets.entrySet()) {
            assertThat(consumer.committed(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }

    @Test
    public void testAsyncRecommit() throws Exception {
        policy = new AsyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofMillis(200), defaultMaxPendingAsyncCommits);
        long nextRecommitNanos = policy.nextRecommitNanos();
        assignPartitions(consumer, toPartitions(range(0, 30).boxed().collect(toList())), 0L);

        final List<ConsumerRecord<Object, Object>> prevRecords = generateConsumedRecords(consumer, toPartitions(range(0, 10).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> previousCommitOffsets = buildCommitOffsets(prevRecords);
        addCompleteRecordsInPolicy(policy, prevRecords);
        assertThat(policy.tryCommit(true))
                .containsExactlyInAnyOrderElementsOf(toPartitions(range(0, 10).boxed().collect(toList())));
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);

        Thread.sleep(200);
        nextRecommitNanos = policy.nextRecommitNanos();
        final List<ConsumerRecord<Object, Object>> newRecords = generateConsumedRecords(consumer, toPartitions(range(10, 20).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> newCommitOffsets = buildCommitOffsets(newRecords);
        newCommitOffsets.putAll(previousCommitOffsets);

        addCompleteRecordsInPolicy(policy, newRecords);
        policy.setForceSync(false);
        assertThat(policy.tryCommit(false)).isEmpty();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : newCommitOffsets.entrySet()) {
            assertThat(consumer.committed(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }

    @Test
    public void testTryCommitAll() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }

    @Test
    public void testForceCommitAfterTooManyPendingAsyncCommits() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        int asyncCommitTimes = pendingRecords.size() - 1;
        policy = new AsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofHours(1), asyncCommitTimes);

        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, asyncCommitTimes)) {
            addCompleteRecordInPolicy(policy, record);
            assertThat(policy.tryCommit(true))
                    .hasSize(1)
                    .isEqualTo(Collections.singleton(new TopicPartition(record.topic(), record.partition())));
            assertThat(policy.forceSync()).isFalse();
        }

        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any());
        verify(mockConsumer, never()).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isEqualTo(asyncCommitTimes);

        final ConsumerRecord<Object, Object> synCommitRecord = pendingRecords.get(asyncCommitTimes);
        addCompleteRecordInPolicy(policy, synCommitRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(synCommitRecord.topic(), synCommitRecord.partition())));
        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any());
        verify(mockConsumer, times(1)).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }

    @Test
    public void testAsyncCommitIntertwineWithSyncCommits() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        policy = new AsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofHours(1), 10);

        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            addCompleteRecordInPolicy(policy, record);
            assertThat(policy.tryCommit(true))
                    .hasSize(1)
                    .isEqualTo(Collections.singleton(new TopicPartition(record.topic(), record.partition())));
        }

        verify(mockConsumer, times(28)).commitAsync(any());
        verify(mockConsumer, times(2)).commitSync();
    }

    @Test
    public void testForceSyncAfterAsyncCommitFailed() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        final Map<TopicPartition, OffsetAndMetadata> mockedOffsets = new HashMap<>();
        final Exception exception = new RuntimeException("expected exception");

        doAnswer(invocation -> {
            OffsetCommitCallback callback = invocation.getArgument(0);
            callback.onComplete(mockedOffsets, exception);
            return null;
        }).when(mockConsumer).commitAsync(any());

        policy = new AsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofHours(1), 10);

        final ConsumerRecord<Object, Object> triggerFailedRecord = pendingRecords.get(0);
        addCompleteRecordInPolicy(policy, triggerFailedRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any());
        verify(mockConsumer, never()).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
        assertThat(policy.forceSync()).isTrue();

        final ConsumerRecord<Object, Object> syncRecord = pendingRecords.get(1);
        addCompleteRecordInPolicy(policy, syncRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(syncRecord.topic(), syncRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any());
        verify(mockConsumer, times(1)).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
        assertThat(policy.forceSync()).isFalse();
    }
}