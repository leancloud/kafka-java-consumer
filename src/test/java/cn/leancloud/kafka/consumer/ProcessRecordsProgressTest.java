package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class ProcessRecordsProgressTest {
    private ProcessRecordsProgress progress;

    @Before
    public void setUp() {
        progress = new ProcessRecordsProgress();
    }

    @Test
    public void testAddOnePendingRecord() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg);
        progress.markPendingRecord(record);
        assertThat(progress.pendingRecordOffsets())
                .hasSize(1)
                .containsOnlyKeys(partition(101))
                .containsValue(1002L);
        assertThat(progress.noCompletedRecords()).isFalse();
        assertThat(progress.noOffsetsToCommit()).isTrue();
    }

    @Test
    public void testAddSeveralPendingRecord() {
        progress.markPendingRecord(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        progress.markPendingRecord(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        progress.markPendingRecord(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        progress.markPendingRecord(new ConsumerRecord<>(testingTopic, 101, 1004, defaultKey, defaultMsg));

        assertThat(progress.pendingRecordOffsets())
                .hasSize(3)
                .containsKeys(partition(101),
                        partition(102), partition(103))
                .containsValues(1005L, 1003L, 1004L)
                .extracting(partition(101))
                .isEqualTo(1005L);
        assertThat(progress.noCompletedRecords()).isFalse();
        assertThat(progress.noOffsetsToCommit()).isTrue();
    }

    @Test
    public void testAddOneCompleteRecord() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg);
        progress.markPendingRecord(record);
        progress.markCompletedRecord(record);
        assertThat(progress.completedOffsetsToCommit())
                .hasSize(1)
                .containsOnlyKeys(partition(101))
                .containsValue(new OffsetAndMetadata(1002L));
    }

    @Test
    public void testAddSeveralCompleteRecord() {
        final List<ConsumerRecord<Object, Object>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 101, 1004, defaultKey, defaultMsg));

        for (ConsumerRecord<Object, Object> record : records) {
            progress.markPendingRecord(record);
            progress.markCompletedRecord(record);
        }

        assertThat(progress.completedOffsetsToCommit())
                .hasSize(3)
                .containsKeys(partition(101), partition(102), partition(103))
                .containsValues(new OffsetAndMetadata(1003L), new OffsetAndMetadata(1004L), new OffsetAndMetadata(1002L))
                .extracting(partition(101))
                // the second added record is not a consecutive record, so the offset is remains after it added
                .isEqualTo(new OffsetAndMetadata(1002L));
    }

    @Test
    public void testRevokePartitions() {
        final List<ConsumerRecord<Object, Object>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 101, 1004, defaultKey, defaultMsg));

        for (ConsumerRecord<Object, Object> record : records) {
            progress.markPendingRecord(record);
            progress.markCompletedRecord(record);
        }

        progress.clearFor(toPartitions(IntStream.range(101, 103).boxed().collect(toList())));

        assertThat(progress.completedOffsetsToCommit())
                .hasSize(1)
                .containsOnlyKeys(partition(103))
                .containsValue(new OffsetAndMetadata(1004L));
    }

    private TopicPartition partition(int partition) {
        return new TopicPartition(testingTopic, partition);
    }

    private OffsetAndMetadata offset(long offset) {
        return new OffsetAndMetadata(offset);
    }
}