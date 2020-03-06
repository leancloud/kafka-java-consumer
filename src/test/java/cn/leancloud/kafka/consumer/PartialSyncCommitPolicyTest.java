package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class PartialSyncCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private PartialSyncCommitPolicy<Object, Object> policy;
    private List<TopicPartition> partitions;
    private List<ConsumerRecord<Object, Object>> pendingRecords;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new PartialSyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofSeconds(30));
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        pendingRecords = generateConsumedRecords(consumer, partitions, 2);
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
        assertThat(policy.tryCommit(false)).isEmpty();
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
        assertThat(policy.tryCommit(false)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
        }
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testNoPendingFuturesLeft() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.markPendingRecord(record);
            policy.markCompletedRecord(record);
        }

        assertThat(policy.tryCommit(true))
                .hasSize(partitions.size())
                .containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(3));
        }

        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.completedTopicOffsetsToCommit()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }

    @Test
    public void testPartialCommit() {
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
    }
}