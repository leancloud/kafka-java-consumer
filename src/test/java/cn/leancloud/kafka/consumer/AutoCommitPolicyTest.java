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

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
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
    public void testOnlyConsumedRecords() {
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
    }

    @Test
    public void testOnlyPendingRecords() {
        addPendingRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.noCompletedOffsets()).isFalse();
        assertThat(policy.topicOffsetHighWaterMark()).isNotEmpty();
    }

    @Test
    public void testHasCompleteRecordsAndPendingRecords() {
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
            // first half of the partitions is completed and cleaned
            // second half of the partitions is not completed and the topic offset mark is still there
            if (partition.partition() < partitions.size() / 2) {
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isNull();
            } else {
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isEqualTo(3);
            }
        }

        assertThat(policy.completedTopicOffsetsToCommit()).isEmpty();
    }

    @Test
    public void testFullCommit() {
        addCompleteRecordsInPolicy(policy, pendingRecords);


        assertThat(policy.tryCommit(true))
                .hasSize(partitions.size())
                .containsExactlyInAnyOrderElementsOf(partitions);

        assertThat(policy.noCompletedOffsets()).isTrue();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
    }
}