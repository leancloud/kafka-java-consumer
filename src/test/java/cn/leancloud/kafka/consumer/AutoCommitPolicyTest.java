package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
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

public class AutoCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private AutoCommitPolicy<Object, Object> policy;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new AutoCommitPolicy<>(consumer);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testNoCompleteRecords() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        preparePendingRecords(partitions, 1);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
    }

    @Test
    public void testPartialComplete() {
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
            // first half of the partitions is completed and cleaned
            // second half of the partitions is not completed and the topic offset mark is still there
            if (partition.partition() < partitions.size() / 2) {
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isNull();
            } else {
                assertThat(policy.topicOffsetHighWaterMark().get(partition)).isEqualTo(3);
            }
        }

        assertThat(policy.completedTopicOffsets()).isEmpty();
    }

    private List<ConsumerRecord<Object, Object>> preparePendingRecords(List<TopicPartition> partitions, int size) {
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, size);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        return pendingRecords;
    }

}