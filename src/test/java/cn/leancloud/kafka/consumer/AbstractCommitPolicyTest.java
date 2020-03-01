package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class AbstractCommitPolicyTest {
    private static class TestingAbstractCommitPolicy extends AbstractCommitPolicy<Object, Object> {
        TestingAbstractCommitPolicy(Consumer<Object, Object> consumer) {
            super(consumer);
        }

        @Override
        public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
            return Collections.emptySet();
        }
    }

    private MockConsumer<Object, Object> consumer;
    private TestingAbstractCommitPolicy policy;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        policy = new TestingAbstractCommitPolicy(consumer);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testAddOnePendingRecord() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg);
        policy.markPendingRecord(record);
        assertThat(policy.topicOffsetHighWaterMark())
                .hasSize(1)
                .containsOnlyKeys(partition(101))
                .containsValue(1002L);
    }

    @Test
    public void testAddSeveralPendingRecord() {
        policy.markPendingRecord(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        policy.markPendingRecord(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        policy.markPendingRecord(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        policy.markPendingRecord(new ConsumerRecord<>(testingTopic, 101, 1004, defaultKey, defaultMsg));

        assertThat(policy.topicOffsetHighWaterMark())
                .hasSize(3)
                .containsKeys(partition(101),
                        partition(102), partition(103))
                .containsValues(1005L, 1003L, 1004L)
                .extracting(partition(101))
                .isEqualTo(1005L);
    }

    @Test
    public void testAddOneCompleteRecord() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg);
        policy.markCompletedRecord(record);
        assertThat(policy.completedTopicOffsets())
                .hasSize(1)
                .containsOnlyKeys(partition(101))
                .containsValue(new OffsetAndMetadata(1002L));
    }

    @Test
    public void testAddSeveralCompleteRecord() {
        policy.markCompletedRecord(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        policy.markCompletedRecord(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        policy.markCompletedRecord(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        policy.markCompletedRecord(new ConsumerRecord<>(testingTopic, 101, 1004, defaultKey, defaultMsg));

        assertThat(policy.completedTopicOffsets())
                .hasSize(3)
                .containsKeys(partition(101),
                        partition(102), partition(103))
                .containsValues(new OffsetAndMetadata(1003L), new OffsetAndMetadata(1004L), new OffsetAndMetadata(1005L))
                .extracting(partition(101))
                .isEqualTo(new OffsetAndMetadata(1005L));
    }

    @Test
    public void testPartialCommit() {
        assignPartitions(consumer, toPartitions(IntStream.range(101, 112).boxed().collect(toList())), 0L);

        final List<ConsumerRecord<Object, Object>> highWaterMark = new ArrayList<>();
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        // not in complete records
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 104, 1005, defaultKey, defaultMsg));
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 101, 1006, defaultKey, defaultMsg));
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 102, 1007, defaultKey, defaultMsg));
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 103, 1008, defaultKey, defaultMsg));
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 101, 1009, defaultKey, defaultMsg));

        final List<ConsumerRecord<Object, Object>> completeRecords = new ArrayList<>();
        completeRecords.add(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        completeRecords.add(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        completeRecords.add(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        completeRecords.add(new ConsumerRecord<>(testingTopic, 101, 1006, defaultKey, defaultMsg));
        // greater than high water mark
        completeRecords.add(new ConsumerRecord<>(testingTopic, 102, 1011, defaultKey, defaultMsg));
        // partition not in high water mark records
        completeRecords.add(new ConsumerRecord<>(testingTopic, 111, 2222, defaultKey, defaultMsg));

        for (ConsumerRecord<Object, Object> record : highWaterMark) {
            consumer.addRecord(record);
            policy.markPendingRecord(record);
        }

        for (ConsumerRecord<Object, Object> record : completeRecords) {
            consumer.addRecord(record);
            policy.markCompletedRecord(record);
        }

        consumer.poll(0);

        assertThat(policy.syncPartialCommit())
                .hasSize(2)
                .containsExactlyInAnyOrder(partition(102), partition(111));
        assertThat(consumer.committed(partition(101))).isEqualTo(offset(1007L));
        assertThat(consumer.committed(partition(102))).isEqualTo(offset(1012L));
        assertThat(consumer.committed(partition(103))).isEqualTo(offset(1004L));
        assertThat(consumer.committed(partition(111))).isEqualTo(offset(2223L));

        assertThat(policy.completedTopicOffsets()).isEmpty();
        assertThat(policy.topicOffsetHighWaterMark())
                .hasSize(3)
                .extracting(partition(101))
                .isEqualTo(1010L);
        assertThat(policy.topicOffsetHighWaterMark())
                .hasSize(3)
                .extracting(partition(103))
                .isEqualTo(1009L);
        assertThat(policy.topicOffsetHighWaterMark())
                .hasSize(3)
                .extracting(partition(104))
                .isEqualTo(1006L);
    }

    private TopicPartition partition(int partition) {
        return new TopicPartition(testingTopic, partition);
    }

    private OffsetAndMetadata offset(long offset) {
        return new OffsetAndMetadata(offset);
    }
}