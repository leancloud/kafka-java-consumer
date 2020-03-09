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

public class SyncCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private SyncCommitPolicy<Object, Object> policy;
    private List<TopicPartition> partitions;
    private List<ConsumerRecord<Object, Object>> pendingRecords;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new SyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofHours(1));
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
    public void testTryCommitAll() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }
}