package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.toPartitions;
import static java.util.stream.Collectors.toList;
import static org.mockito.Mockito.*;

public class RebalanceListenerTest {
    private CommitPolicy<Object, Object> policy;
    private Consumer<Object, Object> consumer;
    private RebalanceListener listener;

    @Before
    public void setUp() {
        policy = mock(CommitPolicy.class);
        consumer = mock(Consumer.class);
        listener = new RebalanceListener(consumer, policy);
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testPauseNotFinishedPartitionsOnPartitionAssign() {
        final List<TopicPartition> pausedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<TopicPartition> partitionToResumeAfterCommit = toPartitions(IntStream.range(0, 20).boxed().collect(toList()));
        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(10, 25).boxed().collect(toList()));
        final List<TopicPartition> partitionStillNeedsToPause = toPartitions(IntStream.range(20, 25).boxed().collect(toList()));

        when(consumer.paused()).thenReturn(new HashSet<>(pausedPartitions));
        when(policy.partialCommit()).thenReturn(new HashSet<>(partitionToResumeAfterCommit));

        listener.onPartitionsRevoked(pausedPartitions);
        listener.onPartitionsAssigned(assignedPartitions);

        verify(consumer, times(1)).pause(new HashSet<>(partitionStillNeedsToPause));
    }
}