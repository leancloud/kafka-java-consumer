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

public class PartialAsyncCommitPolicyTest {
    private static final int defaultMaxPendingAsyncCommits = 10;
    private MockConsumer<Object, Object> consumer;
    private PartialAsyncCommitPolicy<Object, Object> policy;
    private List<TopicPartition> partitions;
    private List<ConsumerRecord<Object, Object>> pendingRecords;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new PartialAsyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofSeconds(30), defaultMaxPendingAsyncCommits);
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0);
        pendingRecords = generateConsumedRecords(consumer, partitions, 2);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testForceSyncFullCommit() {
        policy.setForceSync(true);
        policy.setPendingAsyncCommitCount(100);
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
        }
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
        assertThat(policy.forceSync()).isFalse();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }

    @Test
    public void testForceSyncPartialCommit() {
        policy.setForceSync(true);
        policy.setPendingAsyncCommitCount(100);
        final long nextRecommitNanos = policy.nextRecommitNanos();
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.markPendingRecord(record);

            // complete the first half of the partitions
            if (record.partition() < partitions.size() / 2 && record.offset() < 3) {
                policy.markCompletedRecord(record);
            }
        }

        assertThat(policy.tryCommit(false))
                .hasSize(partitions.size() / 2)
                .containsExactlyInAnyOrderElementsOf(partitions.subList(0, partitions.size() / 2));
        for (TopicPartition partition : partitions) {
            // first half of the partitions is committed and topic offset mark is cleaned
            // second half of the partitions is not committed and topic offset mark is still there
            if (partition.partition() < partitions.size() / 2) {
                assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isNull();
            } else {
                assertThat(consumer.committed(partition)).isNull();
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isEqualTo(3);
            }
        }

        assertThat(policy.completedTopicOffsetsToCommit()).isEmpty();
        assertThat(policy.noCompletedOffsets()).isFalse();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
        assertThat(policy.forceSync()).isFalse();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }

    @Test
    public void testOnlyConsumedRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        assertThat(policy.tryCommit(true)).isEmpty();
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
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.noTopicOffsetsToCommit()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).hasSize(partitions.size());
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testSyncFullCommit() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        policy.setPendingAsyncCommitCount(100);
        addCompleteRecordsInPolicy(policy, pendingRecords);
        policy.setPendingAsyncCommitCount(defaultMaxPendingAsyncCommits);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
        }
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }

    @Test
    public void testAsyncFullCommit() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        policy.setPendingAsyncCommitCount(0);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
        }
        // async commit callback called immediately when we call consumer.commitAsync()
        // so processing record states was cleared
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
        // pending async commit should be 1 but because consumer.commitAsync() for MockConsumer
        // is a synchronous function so when policy.tryCommit() finish, the callback for consumer.commitAsync()
        // was finished too which clears the pendingAsyncCommitCount
        assertThat(policy.pendingAsyncCommitCount()).isEqualTo(0);
    }

    @Test
    public void testPartialSyncCommit() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        policy.setPendingAsyncCommitCount(defaultMaxPendingAsyncCommits);

        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.markPendingRecord(record);
            // complete the first half of the partitions
            if (record.partition() < partitions.size() / 2 && record.offset() < 3) {
                policy.markCompletedRecord(record);
            }
        }

        assertThat(policy.tryCommit(false))
                .hasSize(partitions.size() / 2)
                .containsExactlyInAnyOrderElementsOf(partitions.subList(0, partitions.size() / 2));
        for (TopicPartition partition : partitions) {
            // first half of the partitions is committed and topic offset mark is cleaned
            // second half of the partitions is not committed and topic offset mark is still there
            if (partition.partition() < partitions.size() / 2) {
                assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isNull();
            } else {
                assertThat(consumer.committed(partition)).isNull();
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isEqualTo(3);
            }
        }

        assertThat(policy.noTopicOffsetsToCommit()).isTrue();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }

    @Test
    public void testPartialAsyncCommit() {
        final long nextRecommitNanos = policy.nextRecommitNanos();

        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.markPendingRecord(record);
            // complete the first half of the partitions
            if (record.partition() < partitions.size() / 2 && record.offset() < 3) {
                policy.markCompletedRecord(record);
            }
        }

        assertThat(policy.tryCommit(false))
                .hasSize(partitions.size() / 2)
                .containsExactlyInAnyOrderElementsOf(partitions.subList(0, partitions.size() / 2));
        Map<TopicPartition, OffsetAndMetadata> pendingOffsets = policy.pendingAsyncCommitOffset();
        for (TopicPartition partition : partitions) {
            // first half of the partitions is committed and topic offset mark is cleaned
            // second half of the partitions is not committed and topic offset mark is still there
            if (partition.partition() < partitions.size() / 2) {
                assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isNull();
                assertThat(pendingOffsets).containsEntry(partition, new OffsetAndMetadata(3));
            } else {
                assertThat(consumer.committed(partition)).isNull();
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isEqualTo(3);
                assertThat(pendingOffsets).doesNotContainKey(partition);
            }
        }

        assertThat(policy.noCompletedOffsets()).isFalse();
        assertThat(policy.noTopicOffsetsToCommit()).isTrue();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
        // pending async commit should be 1 but because consumer.commitAsync() for MockConsumer
        // is a synchronous function so when policy.tryCommit() finish, the callback for consumer.commitAsync()
        // was finished too which clears the pendingAsyncCommitCount
        assertThat(policy.pendingAsyncCommitCount()).isEqualTo(0);
    }

    @Test
    public void testMultiPartialAsyncCommit() {
        final Consumer<Object, Object> mockConsumer = mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        final int asyncCommitTimes = pendingRecords.size() - 1;
        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofHours(30), 2 * asyncCommitTimes);

        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, asyncCommitTimes)) {
            final TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            addCompleteRecordInPolicy(policy, record);
            assertThat(policy.tryCommit(false))
                    .hasSize(1)
                    .containsExactly(partition);
            assertThat(policy.tryCommit(false))
                    .hasSize(0);
            assertThat(policy.forceSync()).isFalse();

            final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
            verify(mockConsumer, times(1)).commitAsync(eq(offsets), any());
        }

        assertThat(policy.pendingAsyncCommitCount()).isEqualTo(asyncCommitTimes);
        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any(), any());
        verify(mockConsumer, never()).commitSync();
    }

    @Test
    public void testPartialSyncThenPartialAsyncCommit() {
        final Consumer<Object, Object> mockConsumer = mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofHours(30), defaultMaxPendingAsyncCommits);

        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, defaultMaxPendingAsyncCommits)) {
            final TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            addCompleteRecordInPolicy(policy, record);
            policy.setPendingAsyncCommitCount(defaultMaxPendingAsyncCommits);
            assertThat(policy.tryCommit(false))
                    .hasSize(1)
                    .containsExactly(partition);
            assertThat(policy.tryCommit(false))
                    .hasSize(0);
            assertThat(policy.forceSync()).isFalse();

            final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(partition, new OffsetAndMetadata(record.offset() + 1));
            verify(mockConsumer, times(1)).commitSync(eq(offsets));
            assertThat(policy.pendingAsyncCommitCount()).isZero();
        }

        verify(mockConsumer, times(defaultMaxPendingAsyncCommits)).commitSync(any());
        verify(mockConsumer, never()).commitAsync(any());
        verify(mockConsumer, never()).commitAsync(any(), any());
    }

    @Test
    public void testSyncCommitAfterTooManyPendingAsyncCommits() {
        final Consumer<Object, Object> mockConsumer = mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        final int asyncCommitTimes = pendingRecords.size() - 1;
        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofSeconds(30), asyncCommitTimes);

        final Set<TopicPartition> partitionsToResume = new HashSet<>();
        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, asyncCommitTimes)) {
            partitionsToResume.add(new TopicPartition(record.topic(), record.partition()));
            addCompleteRecordInPolicy(policy, record);
            assertThat(policy.tryCommit(false))
                    .hasSize(1)
                    .containsExactly(new TopicPartition(record.topic(), record.partition()));
            assertThat(policy.forceSync()).isFalse();
        }

        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any(), any());
        verify(mockConsumer, never()).commitSync();
        assertThat(policy.pendingAsyncCommitCount()).isEqualTo(asyncCommitTimes);

        final ConsumerRecord<Object, Object> synCommitRecord = pendingRecords.get(asyncCommitTimes);
        partitionsToResume.add(new TopicPartition(synCommitRecord.topic(), synCommitRecord.partition()));
        addCompleteRecordInPolicy(policy, synCommitRecord);
        assertThat(policy.tryCommit(false))
                .hasSize(partitionsToResume.size())
                .isEqualTo(partitionsToResume);
        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any(), any());
        verify(mockConsumer, times(1)).commitSync(any());
        assertThat(policy.pendingAsyncCommitCount()).isZero();
        assertThat(policy.forceSync()).isFalse();
    }

    @Test
    public void testForceSyncAfterAsyncPartialCommitFailed() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        final Exception exception = new RuntimeException("expected exception");

        doAnswer(invocation -> {
            final Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
            final OffsetCommitCallback callback = invocation.getArgument(1);
            callback.onComplete(offsets, exception);
            return null;
        }).when(mockConsumer).commitAsync(any(), any());

        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofSeconds(30), 10);

        // a failed async commit on the first time
        final ConsumerRecord<Object, Object> triggerFailedRecord = pendingRecords.get(0);
        addCompleteRecordInPolicy(policy, triggerFailedRecord);
        assertThat(policy.tryCommit(false))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any(), any());
        verify(mockConsumer, never()).commitSync(any());
        assertThat(policy.forceSync()).isTrue();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }

    @Test
    public void testForceSyncAfterAsyncFullCommitFailed() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        final Exception exception = new RuntimeException("expected exception");
        // a failed async commit on the first time
        final ConsumerRecord<Object, Object> triggerFailedRecord = pendingRecords.get(0);

        doAnswer(invocation -> {
            final OffsetCommitCallback callback = invocation.getArgument(0);
            final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
            offsets.put(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition()), new OffsetAndMetadata(triggerFailedRecord.offset()));
            callback.onComplete(offsets, exception);
            return null;
        }).when(mockConsumer).commitAsync(any());

        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ZERO, 3, Duration.ofSeconds(30), 10);

        addCompleteRecordInPolicy(policy, triggerFailedRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any());
        verify(mockConsumer, never()).commitSync(any());
        assertThat(policy.forceSync()).isTrue();
        assertThat(policy.pendingAsyncCommitCount()).isZero();
    }
}