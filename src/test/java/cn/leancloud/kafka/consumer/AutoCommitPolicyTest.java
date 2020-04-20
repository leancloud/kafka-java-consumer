package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class AutoCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private AutoCommitPolicy<Object, Object> policy;
    private List<TopicPartition> partitions;
    private List<ConsumerRecord<Object, Object>> pendingRecords;
    private ProcessRecordsProgress progress;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        progress = new ProcessRecordsProgress();
        policy = new AutoCommitPolicy<>(consumer);
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        pendingRecords = generateConsumedRecords(consumer, partitions, 2);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testPauseResumeCommit() {
        addCompleteRecordsInPolicy(progress, pendingRecords);

        policy.pauseCommit();
        assertThat(policy.tryCommit(true, progress)).isEmpty();
        assertThat(progress.noCompletedRecords()).isFalse();
        assertThat(progress.noPendingRecords()).isFalse();

        policy.resumeCommit();
        assertThat(policy.tryCommit(true, progress))
                .hasSize(partitions.size())
                .containsExactlyInAnyOrderElementsOf(partitions);

        assertThat(progress.noCompletedRecords()).isTrue();
        assertThat(progress.noPendingRecords()).isTrue();
    }

    @Test
    public void testOnlyConsumedRecords() {
        assertThat(policy.tryCommit(true, progress)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(progress.noCompletedRecords()).isTrue();
        assertThat(progress.noPendingRecords()).isTrue();
    }

    @Test
    public void testOnlyPendingRecords() {
        addPendingRecordsInPolicy(progress, pendingRecords);
        assertThat(policy.tryCommit(false, progress)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(progress.noCompletedRecords()).isFalse();
        assertThat(progress.noPendingRecords()).isFalse();
    }

    @Test
    public void testHasCompleteRecordsAndPendingRecords() {
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            progress.markPendingRecord(record);

            // complete the first half of the partitions
            if (record.partition() < partitions.size() / 2 && record.offset() < 3) {
                progress.markCompletedRecord(record);
            }
        }

        assertThat(policy.tryCommit(false, progress))
                .hasSize(partitions.size() / 2)
                .containsExactlyInAnyOrderElementsOf(partitions.subList(0, partitions.size() / 2));
        for (TopicPartition partition : partitions) {
            // first half of the partitions is completed and cleaned
            // second half of the partitions is not completed and the topic offset mark is still there
            if (partition.partition() < partitions.size() / 2) {
                assertThat(progress.pendingRecordOffsets().get(partition)).isNull();
            } else {
                assertThat(progress.pendingRecordOffsets().get(partition)).isEqualTo(3);
            }
        }

        assertThat(progress.completedOffsetsToCommit()).isEmpty();
    }

    @Test
    public void testFullCommit() {
        addCompleteRecordsInPolicy(progress, pendingRecords);

        assertThat(policy.tryCommit(true, progress))
                .hasSize(partitions.size())
                .containsExactlyInAnyOrderElementsOf(partitions);

        assertThat(progress.noCompletedRecords()).isTrue();
        assertThat(progress.noPendingRecords()).isTrue();
    }
}