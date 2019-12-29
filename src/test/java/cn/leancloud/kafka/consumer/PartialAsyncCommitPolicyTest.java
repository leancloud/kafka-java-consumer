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
        policy = new PartialAsyncCommitPolicy<>(consumer, Duration.ofSeconds(30), defaultMaxPendingAsyncCommits);
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        pendingRecords = preparePendingRecords(partitions, 1);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testNoCompleteRecords() {
        preparePendingRecords(partitions, 1);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
    }

    @Test
    public void testPartialAsyncCommit() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = preparePendingRecords(partitions, 2);
        // two records for each partitions
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.addPendingRecord(record);
        }

        // complete the first half of the partitions
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            if (record.partition() < partitions.size() / 2 && record.offset() < 3) {
                policy.completeRecord(record);
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

        assertThat(policy.completedTopicOffsets()).isEmpty();
    }

    @Test
    public void testPartialAsyncCommitWithNoPendingFuturesLeft() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = preparePendingRecords(partitions, 2);
        // two records for each partitions
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.addPendingRecord(record);
            policy.completeRecord(record);
        }

        assertThat(policy.tryCommit(true))
                .hasSize(partitions.size())
                .containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
        }

        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.completedTopicOffsets()).isEmpty();
    }

    @Test
    public void testForceCommitAfterTooManyPendingAsyncCommits() {
        final Consumer<Object, Object> mockConsumer = mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        final int asyncCommitTimes = pendingRecords.size() - 1;
        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ofSeconds(30), asyncCommitTimes);

        final Set<TopicPartition> partitionsToResume = new HashSet<>();
        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, asyncCommitTimes)) {
            partitionsToResume.add(new TopicPartition(record.topic(), record.partition()));
            completeRecord(record);
            assertThat(policy.tryCommit(true))
                    .hasSize(partitionsToResume.size())
                    .isEqualTo(partitionsToResume);
        }

        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any(), any());
        verify(mockConsumer, never()).commitSync();

        final ConsumerRecord<Object, Object> synCommitRecord = pendingRecords.get(asyncCommitTimes);
        partitionsToResume.add(new TopicPartition(synCommitRecord.topic(), synCommitRecord.partition()));
        completeRecord(synCommitRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(partitionsToResume.size())
                .isEqualTo(partitionsToResume);
        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any(), any());
        verify(mockConsumer, times(1)).commitSync(any());
    }

    @Test
    public void testAsyncCommitIntertwineWithSyncCommits() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ofSeconds(30), 10);

        final Set<TopicPartition> partitionsToResume = new HashSet<>();
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            if (partitionsToResume.size() == 11) {
                partitionsToResume.clear();
            }
            partitionsToResume.add(new TopicPartition(record.topic(), record.partition()));

            completeRecord(record);
            assertThat(policy.tryCommit(true))
                    .hasSize(partitionsToResume.size())
                    .isEqualTo(partitionsToResume);
        }

        verify(mockConsumer, times(28)).commitAsync(any(), any());
        verify(mockConsumer, times(2)).commitSync(any());
    }

    @Test
    public void testForceSyncAfterAsyncCommitFailed() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        final Exception exception = new RuntimeException("expected exception");

        doAnswer(invocation -> {
            final Map<TopicPartition, OffsetAndMetadata> offsets = invocation.getArgument(0);
            final OffsetCommitCallback callback = invocation.getArgument(1);
            callback.onComplete(offsets, exception);
            return null;
        }).when(mockConsumer).commitAsync(any(), any());

        policy = new PartialAsyncCommitPolicy<>(mockConsumer, Duration.ofSeconds(30), 10);

        // a failed async commit on the first time
        final ConsumerRecord<Object, Object> triggerFailedRecord = pendingRecords.get(0);
        completeRecord(triggerFailedRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any(), any());
        verify(mockConsumer, never()).commitSync(any());

        // sync commit after the failed async commit
        final ConsumerRecord<Object, Object> syncRecord = pendingRecords.get(1);
        completeRecord(syncRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(2)
                .containsExactlyInAnyOrderElementsOf(Arrays.asList(
                        new TopicPartition(syncRecord.topic(), syncRecord.partition()),
                        new TopicPartition(syncRecord.topic(), triggerFailedRecord.partition())
                ));

        verify(mockConsumer, times(1)).commitAsync(any(), any());
        verify(mockConsumer, times(1)).commitSync(any());
    }

    private void completeRecord(ConsumerRecord<Object, Object> record) {
        policy.addPendingRecord(record);
        policy.completeRecord(record);
    }

    private List<ConsumerRecord<Object, Object>> preparePendingRecords(List<TopicPartition> partitions, int size) {
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, size);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        return pendingRecords;
    }
}