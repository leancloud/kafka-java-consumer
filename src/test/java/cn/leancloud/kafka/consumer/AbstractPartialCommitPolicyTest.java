package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.function.BinaryOperator.maxBy;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class AbstractPartialCommitPolicyTest {
    private static class TestingPartialCommitPolicy extends AbstractPartialCommitPolicy<Object, Object> {
        TestingPartialCommitPolicy(Consumer<Object, Object> consumer, Duration forceWholeCommitInterval) {
            super(consumer, forceWholeCommitInterval);
        }

        @Override
        public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
            throw new UnsupportedOperationException();
        }
    }

    private MockConsumer<Object, Object> consumer;
    private AbstractPartialCommitPolicy<Object, Object> policy;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
    }

    @Test
    public void testPartialCommit() {
        policy = new TestingPartialCommitPolicy(consumer, Duration.ofHours(1));
        final List<TopicPartition> partitions = toPartitions(range(0, 30).boxed().collect(toList()));
        final List<ConsumerRecord<Object, Object>> pendingRecords = prepareConsumerRecords(partitions, 1, 10);
        final List<ConsumerRecord<Object, Object>> completedRecords =
                completeRecords(pendingRecords.subList(0, pendingRecords.size() / 2));

        final Map<TopicPartition, OffsetAndMetadata> completeOffsets = buildCommitOffsets(completedRecords);

        assertThat(policy.offsetsToPartialCommit()).isEqualTo(completeOffsets);
    }

    @Test
    public void testWholeCommit() throws Exception {
        policy = new TestingPartialCommitPolicy(consumer, Duration.ofMillis(200));
        final Map<TopicPartition, OffsetAndMetadata> previousCommitOffsets =
                commitRecords(completeRecords(prepareConsumerRecords(toPartitions(range(0, 10).boxed().collect(toList())), 1, 10)));

        policy.offsetsToPartialCommit().clear();
        policy.topicOffsetHighWaterMark().clear();
        final Map<TopicPartition, OffsetAndMetadata> newOffsetsToCommit =
                buildCommitOffsets(completeRecords(prepareConsumerRecords(toPartitions(range(10, 20).boxed().collect(toList())), 1, 10)));


        Thread.sleep(200);
        assertThat(policy.offsetsToPartialCommit())
                .hasSize(previousCommitOffsets.size() + newOffsetsToCommit.size())
                .containsAllEntriesOf(previousCommitOffsets)
                .containsAllEntriesOf(newOffsetsToCommit);
    }

    private List<ConsumerRecord<Object, Object>> completeRecords(List<ConsumerRecord<Object, Object>> records) {
        for (ConsumerRecord<Object, Object> record : records) {
            policy.addPendingRecord(record);
            policy.completeRecord(record);
        }
        return records;
    }

    private Map<TopicPartition, OffsetAndMetadata> commitRecords(List<ConsumerRecord<Object, Object>> records) {
        final Map<TopicPartition, OffsetAndMetadata> offsets = buildCommitOffsets(records);
        assignPartitions(consumer, offsets.keySet(), 0L);
        fireConsumerRecords(consumer, records);
        consumer.poll(0);
        consumer.commitSync(offsets);
        return offsets;
    }

    private Map<TopicPartition, OffsetAndMetadata> buildCommitOffsets(List<ConsumerRecord<Object, Object>> records) {
        final Map<TopicPartition, OffsetAndMetadata> completeOffsets = new HashMap<>();
        for (ConsumerRecord<Object, Object> record : records) {
            completeOffsets.merge(new TopicPartition(testingTopic, record.partition()),
                    new OffsetAndMetadata(record.offset() + 1),
                    maxBy(comparing(OffsetAndMetadata::offset)));
        }
        return completeOffsets;
    }
}