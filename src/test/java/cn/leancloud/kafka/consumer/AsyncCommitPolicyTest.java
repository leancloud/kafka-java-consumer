package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
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
    public void testOnlyConsumedRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testOnlyPendingRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addPendingRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.noTopicOffsetsToCommit()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isNotEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testHasCompleteRecordsAndPendingRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.noCompletedOffsets()).isFalse();
        assertThat(policy.topicOffsetHighWaterMark()).isNotEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testTryCommitAllAsync() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        // async commit callback called immediately when we call consumer.commitAsync()
        // so processing record states was cleared
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }

    @Test
    public void testForceCommitAfterTooManyPendingAsyncCommits() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        final int maxAsyncCommitTimes = pendingRecords.size() - 1;
        policy = new AsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3,
                Duration.ofHours(1), maxAsyncCommitTimes);

        final Set<TopicPartition> committedPartitions = new HashSet<>();
        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, maxAsyncCommitTimes)) {
            addCompleteRecordInPolicy(policy, record);
            committedPartitions.add(new TopicPartition(record.topic(), record.partition()));
            assertThat(policy.tryCommit(true))
                    .hasSize(committedPartitions.size())
                    .containsExactlyInAnyOrderElementsOf(committedPartitions);
            assertThat(policy.forceSync()).isFalse();
        }

        verify(mockConsumer, times(maxAsyncCommitTimes)).commitAsync(any());
        verify(mockConsumer, never()).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isEqualTo(maxAsyncCommitTimes);

        final ConsumerRecord<Object, Object> synCommitRecord = pendingRecords.get(maxAsyncCommitTimes);
        addCompleteRecordInPolicy(policy, synCommitRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(partitions.size())
                .containsExactlyInAnyOrderElementsOf(partitions);
        verify(mockConsumer, times(maxAsyncCommitTimes)).commitAsync(any());
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
            policy.tryCommit(true);
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
        final Set<TopicPartition> committedPartitions = new HashSet<>();
        committedPartitions.add(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition()));
        committedPartitions.add(new TopicPartition(syncRecord.topic(), syncRecord.partition()));
        addCompleteRecordInPolicy(policy, syncRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(committedPartitions.size())
                .containsExactlyInAnyOrderElementsOf(committedPartitions);

        verify(mockConsumer, times(1)).commitAsync(any());
        verify(mockConsumer, times(1)).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
        assertThat(policy.forceSync()).isFalse();
    }
}