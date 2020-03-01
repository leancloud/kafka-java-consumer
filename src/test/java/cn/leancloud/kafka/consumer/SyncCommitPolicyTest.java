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

public class SyncCommitPolicyTest {
    private MockConsumer<Object, Object> consumer;
    private SyncCommitPolicy<Object, Object> policy;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new SyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofHours(1));
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testHavePendingRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        final List<ConsumerRecord<Object, Object>> pendingRecords = generateConsumedRecords(consumer, partitions);
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(false)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.completedTopicOffsets()).isNotEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isNotEmpty();
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testNoCompleteRecords() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        generateConsumedRecords(consumer, partitions);
        assertThat(policy.tryCommit(true)).isEmpty();
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isNull();
        }
        assertThat(policy.nextRecommitNanos()).isEqualTo(nextRecommitNanos);
    }

    @Test
    public void testTryCommitAll() {
        final long nextRecommitNanos = policy.nextRecommitNanos();
        final List<TopicPartition> partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        assignPartitions(consumer, partitions, 0L);
        final List<ConsumerRecord<Object, Object>> pendingRecords = generateConsumedRecords(consumer, partitions);
        addCompleteRecordsInPolicy(policy, pendingRecords);
        assertThat(policy.tryCommit(true)).containsExactlyInAnyOrderElementsOf(partitions);
        for (TopicPartition partition : partitions) {
            assertThat(consumer.committed(partition)).isEqualTo(new OffsetAndMetadata(2));
        }
        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.topicOffsetHighWaterMark()).isEmpty();
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }

    @Test
    public void testRecommit() throws Exception{
        policy = new SyncCommitPolicy<>(consumer, Duration.ZERO, 3, Duration.ofMillis(200));
        long nextRecommitNanos = policy.nextRecommitNanos();
        assignPartitions(consumer, toPartitions(range(0, 30).boxed().collect(toList())), 0L);

        final List<ConsumerRecord<Object, Object>> prevRecords = generateConsumedRecords(consumer, toPartitions(range(0, 10).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> previousCommitOffsets = buildCommitOffsets(prevRecords);
        addCompleteRecordsInPolicy(policy, prevRecords);
        assertThat(policy.tryCommit(true))
                .containsExactlyInAnyOrderElementsOf(toPartitions(range(0, 10).boxed().collect(toList())));
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);

        Thread.sleep(200);
        nextRecommitNanos = policy.nextRecommitNanos();
        final List<ConsumerRecord<Object, Object>> newRecords = generateConsumedRecords(consumer, toPartitions(range(10, 20).boxed().collect(toList())), 10);
        final Map<TopicPartition, OffsetAndMetadata> newCommitOffsets = buildCommitOffsets(newRecords);
        newCommitOffsets.putAll(previousCommitOffsets);

        addCompleteRecordsInPolicy(policy, newRecords);
        assertThat(policy.tryCommit(false)).isEmpty();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : newCommitOffsets.entrySet()) {
            assertThat(consumer.committed(entry.getKey())).isEqualTo(entry.getValue());
        }
        assertThat(policy.nextRecommitNanos()).isGreaterThan(nextRecommitNanos);
    }
}