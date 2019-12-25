package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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

public class SyncCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private SyncCommitPolicy<Object, Object> policy;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new SyncCommitPolicy<>(consumer);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testHavePendingRecords() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareRecords(partitions);
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.addPendingRecord(record);
            policy.completeRecord(record);
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
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        prepareRecords(partitions);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
    }

    @Test
    public void testTryCommitAll() {
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareRecords(partitions);
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.addPendingRecord(record);
            policy.completeRecord(record);
        }
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
    }

    private List<ConsumerRecord<Object, Object>> prepareRecords(List<TopicPartition> partitions) {
        // one msg for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 1);
        assignPartitions(consumer, partitions, 0L);
        fireConsumerRecords(consumer, pendingRecords);
        consumer.poll(0);
        return pendingRecords;
    }
}