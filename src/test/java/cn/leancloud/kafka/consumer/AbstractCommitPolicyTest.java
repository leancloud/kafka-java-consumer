package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class AbstractCommitPolicyTest {
    private static class TestingAbstractCommitPolicy extends AbstractCommitPolicy<Object, Object> {
        TestingAbstractCommitPolicy(Consumer<Object, Object> consumer) {
            this(consumer, Duration.ZERO, 3);
        }

        TestingAbstractCommitPolicy(Consumer<Object, Object> consumer, Duration syncCommitRetryInterval, int maxAttemptsForEachSyncCommit) {
            super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit);
        }

        @Override
        public Set<TopicPartition> tryCommit(boolean noPendingRecords, ProcessRecordsProgress progress) {
            return Collections.emptySet();
        }
    }

    private MockConsumer<Object, Object> consumer;
    private ProcessRecordsProgress progress;
    private TestingAbstractCommitPolicy policy;
    private long sleptTime;

    @Before
    public void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        progress = new ProcessRecordsProgress();
        policy = new TestingAbstractCommitPolicy(consumer);
        sleptTime = 0L;
        AbstractCommitPolicy.sleepFunction = sleep -> {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException();
            }
            sleptTime += sleep;
        };
    }

    @After
    public void tearDown() {
        consumer.close();
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
        highWaterMark.add(new ConsumerRecord<>(testingTopic, 105, 1006, defaultKey, defaultMsg));

        final List<ConsumerRecord<Object, Object>> completeRecords = new ArrayList<>();
        completeRecords.add(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        completeRecords.add(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        completeRecords.add(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));

        for (ConsumerRecord<Object, Object> record : highWaterMark) {
            consumer.addRecord(record);
            progress.markPendingRecord(record);
        }

        for (ConsumerRecord<Object, Object> record : completeRecords) {
            consumer.addRecord(record);
            progress.markCompletedRecord(record);
        }

        consumer.poll(0);

        assertThat(policy.partialCommitSync(progress))
                .hasSize(3)
                .containsExactlyInAnyOrder(partition(103), partition(102), partition(101));
        assertThat(consumer.committed(partition(101))).isEqualTo(offset(1002L));
        assertThat(consumer.committed(partition(102))).isEqualTo(offset(1003L));
        assertThat(consumer.committed(partition(103))).isEqualTo(offset(1004L));

        assertThat(progress.completedOffsetsToCommit()).isEmpty();
        assertThat(progress.pendingRecordOffsets())
                .hasSize(2)
                .containsEntry(partition(104), 1006L)
                .containsEntry(partition(105), 1007L);
    }

    @Test
    public void testFullCommit() {
        final Consumer<Object, Object> mockedConsumer = mock(Consumer.class);
        final Duration retryInterval = Duration.ofSeconds(1);
        final int maxAttempts = 3;
        policy = new TestingAbstractCommitPolicy(mockedConsumer, retryInterval, maxAttempts);
        final List<ConsumerRecord<Object, Object>> records = new ArrayList<>();
        records.add(new ConsumerRecord<>(testingTopic, 101, 1001, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 102, 1002, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 103, 1003, defaultKey, defaultMsg));
        records.add(new ConsumerRecord<>(testingTopic, 101, 1004, defaultKey, defaultMsg));

        for (ConsumerRecord<Object, Object> record : records) {
            progress.markPendingRecord(record);
            progress.markCompletedRecord(record);
        }

        assertThat(policy.fullCommitSync(progress))
                .containsExactlyInAnyOrderElementsOf(
                        records.stream()
                                .map(record -> new TopicPartition(testingTopic, record.partition()))
                                .collect(Collectors.toSet()));
        verify(mockedConsumer, times(1)).commitSync();
        assertThat(progress.noCompletedRecords());
        assertThat(progress.noPendingRecords()).isTrue();
    }

    @Test
    public void testRetrySyncCommit() {
        final Consumer<Object, Object> mockedConsumer = mock(Consumer.class);
        final RetriableException exception = new RetriableCommitFailedException("sync commit failed");
        final Duration retryInterval = Duration.ofSeconds(1);
        final int maxAttempts = 3;
        policy = new TestingAbstractCommitPolicy(mockedConsumer, retryInterval, maxAttempts);

        doThrow(exception).when(mockedConsumer).commitSync();
        assertThatThrownBy(() -> policy.commitSyncWithRetry()).isSameAs(exception);

        verify(mockedConsumer, times(maxAttempts)).commitSync();
        assertThat(sleptTime).isEqualTo(retryInterval.multipliedBy(maxAttempts - 1).toMillis());
    }

    @Test
    public void testRetrySyncCommit2() {
        final Consumer<Object, Object> mockedConsumer = mock(Consumer.class);
        final RetriableException exception = new RetriableCommitFailedException("sync commit failed");
        final Duration retryInterval = Duration.ofSeconds(1);
        final int maxAttempts = 3;
        final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        policy = new TestingAbstractCommitPolicy(mockedConsumer, retryInterval, maxAttempts);

        doThrow(exception).when(mockedConsumer).commitSync(offsets);
        assertThatThrownBy(() -> policy.commitSyncWithRetry(offsets)).isSameAs(exception);

        verify(mockedConsumer, times(maxAttempts)).commitSync(offsets);
        assertThat(sleptTime).isEqualTo(retryInterval.multipliedBy(maxAttempts - 1).toMillis());
    }

    @Test
    public void testInterruptOnRetry() {
        final Consumer<Object, Object> mockedConsumer = mock(Consumer.class);
        final RetriableException exception = new RetriableCommitFailedException("sync commit failed");
        final Duration retryInterval = Duration.ofSeconds(1);
        final int maxAttempts = 3;
        policy = new TestingAbstractCommitPolicy(mockedConsumer, retryInterval, maxAttempts);

        Thread.currentThread().interrupt();
        doThrow(exception).when(mockedConsumer).commitSync();
        assertThatThrownBy(() -> policy.commitSyncWithRetry()).isSameAs(exception).satisfies(t -> {
            final Throwable[] suppressed = t.getSuppressed();
            assertThat(suppressed.length).isEqualTo(1);
            assertThat(suppressed[0]).isInstanceOf(InterruptedException.class);
        });

        verify(mockedConsumer, times(1)).commitSync();
        assertThat(sleptTime).isEqualTo(0);
        assertThat(Thread.interrupted());
    }

    private TopicPartition partition(int partition) {
        return new TopicPartition(testingTopic, partition);
    }

    private OffsetAndMetadata offset(long offset) {
        return new OffsetAndMetadata(offset);
    }
}