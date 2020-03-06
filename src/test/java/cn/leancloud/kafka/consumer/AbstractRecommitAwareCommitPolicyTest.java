package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.*;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class AbstractRecommitAwareCommitPolicyTest {
    private static class TestingPolicy extends AbstractRecommitAwareCommitPolicy<Object, Object> {
        TestingPolicy(Consumer<Object, Object> consumer, Duration syncCommitRetryInterval, int maxAttemptsForEachSyncCommit, Duration recommitInterval) {
            super(consumer, syncCommitRetryInterval, maxAttemptsForEachSyncCommit, recommitInterval);
        }

        @Override
        Set<TopicPartition> tryCommit0(boolean noPendingRecords) {
            return null;
        }
    }

    private TestingPolicy policy;
    private List<TopicPartition> partitions;

    @Before
    public void setUp() {
        partitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
    }

    @Test
    public void testRecommit() {
        final Consumer<Object, Object> consumer = mock(Consumer.class);
        final List<ConsumerRecord<Object, Object>> prevRecords = prepareConsumerRecords(toPartitions(IntStream.range(0, 10).boxed().collect(toList())), 1, 10);
        final Map<TopicPartition, OffsetAndMetadata> previousCommitOffsets = buildCommitOffsets(prevRecords);
        final long now = System.nanoTime();

        when(consumer.assignment()).thenReturn(new HashSet<>(partitions));
        when(consumer.committed(any())).thenAnswer(invocation ->
                previousCommitOffsets.get(invocation.getArgument(0))
        );

        policy = new TestingPolicy(consumer, Duration.ZERO, 3, Duration.ofMillis(200));

        policy.updateNextRecommitTime(now - Duration.ofMillis(200).toNanos());
        policy.tryCommit(true);

        verify(consumer, times(1)).commitSync(previousCommitOffsets);
        assertThat(policy.nextRecommitNanos()).isGreaterThan(now + Duration.ofMillis(200).toNanos());
    }
}