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
    private ProcessRecordsProgress progress;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        progress = new ProcessRecordsProgress();
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
    public void testEmptyProgress() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        assertThat(policy.tryCommit(true, progress)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(progress.noCompletedRecords()).isTrue();
        assertThat(progress.noPendingRecords()).isTrue();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testOnlyPendingRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addPendingRecordsInPolicy(progress, pendingRecords);
        assertThat(policy.tryCommit(false, progress)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(progress.noOffsetsToCommit()).isTrue();
        assertThat(progress.pendingRecordOffsets()).hasSize(partitions.size());
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testHasCompleteRecordsAndPendingRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(progress, pendingRecords);
        assertThat(policy.tryCommit(false, progress)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(progress.noCompletedRecords()).isFalse();
        assertThat(progress.noPendingRecords()).isFalse();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testTryCommitAll() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        addCompleteRecordsInPolicy(progress, pendingRecords);
        assertThat(policy.tryCommit(true, progress)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        assertThat(progress.noCompletedRecords()).isTrue();
        assertThat(progress.noPendingRecords()).isTrue();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }
}