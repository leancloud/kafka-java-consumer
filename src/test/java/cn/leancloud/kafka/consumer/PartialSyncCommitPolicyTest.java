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
import java.util.Map;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

public class PartialSyncCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private PartialSyncCommitPolicy<Object, Object> policy;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new PartialSyncCommitPolicy<>(consumer, Duration.ofSeconds(30));
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testNoCompleteRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        generateConsumedRecords(consumer, partitions, 1);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testPartialCommit() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        final List<ConsumerRecord<Object, Object>> pendingRecords = generateConsumedRecords(consumer, partitions, 2);
        // two records for each partitions
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
            policy.markPendingRecord(record);
        }

        // complete the first half of the partitions
        for (ConsumerRecord<Object, Object> record : pendingRecords) {
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

        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testNoPendingFuturesLeft() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        // two records for each partitions
        final List<ConsumerRecord<Object, Object>> pendingRecords = generateConsumedRecords(consumer, partitions,2);
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
        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testRecommit() throws Exception{
        policy = new PartialSyncCommitPolicy<>(consumer, Duration.ofMillis(200));
        long nextRecommitNanos = policy.nextRecommitNanos();
        assignPartitions(consumer, toPartitions(range(0, 30).boxed().collect(toList())), 0L);

        final List<ConsumerRecord<Object, Object>> prevRecords = generateConsumedRecords(consumer, toPartitions(range(0, 10).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> previousCommitOffsets = buildCommitOffsets(prevRecords);
        addCompleteRecordsInPolicy(policy, prevRecords);
        assertThat(policy.tryCommit(true))
                .containsExactlyInAnyOrderElementsOf(toPartitions(range(0, 10).boxed().collect(toList())));
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);

        Thread.sleep(200);
        nextRecommitNanos = policy.nextRecommitNanos();
        final List<ConsumerRecord<Object, Object>> newRecords = generateConsumedRecords(consumer, toPartitions(range(10, 20).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> newCommitOffsets = buildCommitOffsets(newRecords);
        newCommitOffsets.putAll(previousCommitOffsets);

        addCompleteRecordsInPolicy(policy, newRecords);
        assertThat(policy.tryCommit(false))
                .containsExactlyInAnyOrderElementsOf(toPartitions(range(10, 20).boxed().collect(toList())));

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : newCommitOffsets.entrySet()) {
            assertThat(consumer.committed(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }
}