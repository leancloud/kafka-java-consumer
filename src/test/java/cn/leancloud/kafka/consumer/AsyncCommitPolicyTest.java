package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        policy = new AsyncCommitPolicy<>(consumer, defaultMaxPendingAsyncCommits);
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        pendingRecords = prepareRecords(partitions);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testHavePendingRecords() {
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            completeRecord(record);
        }
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.completedTopicOffsets()).isNotEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isNotEmpty();
    }

    @Test
    public void testNoCompleteRecords() {
        prepareRecords(partitions);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
    }

    @Test
    public void testTryCommitAll() {
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            completeRecord(record);
        }
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
    }

    @Test
    public void testForceCommitAfterTooManyPendingAsyncCommits() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        int asyncCommitTimes = pendingRecords.size() - 1;
        policy = new AsyncCommitPolicy<>(mockConsumer, asyncCommitTimes);

        for (ConsumerRecord<Object, Object> record : pendingRecords.subList(0, asyncCommitTimes)) {
            completeRecord(record);
            assertThat(policy.tryCommit(true))
                    .hasSize(1)
                    .isEqualTo(Collections.singleton(new TopicPartition(record.topic(), record.partition())));
        }

        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any());
        verify(mockConsumer, never()).commitSync();

        final ConsumerRecord<Object, Object> synCommitRecord = pendingRecords.get(asyncCommitTimes);
        completeRecord(synCommitRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(synCommitRecord.topic(), synCommitRecord.partition())));
        verify(mockConsumer, times(asyncCommitTimes)).commitAsync(any());
        verify(mockConsumer, times(1)).commitSync();
    }

    @Test
    public void testAsyncCommitIntertwineWithSyncCommits() {
        final Consumer<Object, Object> mockConsumer = Mockito.mock(Consumer.class);
        doNothing().when(mockConsumer).commitAsync(any());

        policy = new AsyncCommitPolicy<>(mockConsumer, 10);

        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            completeRecord(record);
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

        policy = new AsyncCommitPolicy<>(mockConsumer, 10);

        final ConsumerRecord<Object, Object> triggerFailedRecord = pendingRecords.get(0);
        completeRecord(triggerFailedRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(triggerFailedRecord.topic(), triggerFailedRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any());
        verify(mockConsumer, never()).commitSync();

        final ConsumerRecord<Object, Object> syncRecord = pendingRecords.get(1);
        completeRecord(syncRecord);
        assertThat(policy.tryCommit(true))
                .hasSize(1)
                .isEqualTo(Collections.singleton(new TopicPartition(syncRecord.topic(), syncRecord.partition())));

        verify(mockConsumer, times(1)).commitAsync(any());
        verify(mockConsumer, times(1)).commitSync();
    }

    private List<ConsumerRecord<Object, Object>> prepareRecords(List<TopicPartition> partitions) {
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        return pendingRecords;
    }

    private void completeRecord(ConsumerRecord<Object, Object> record) {
        policy.addPendingRecord(record);
        policy.completeRecord(record);
    }

}