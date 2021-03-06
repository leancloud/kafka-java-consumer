package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.toPartitions;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class RebalanceListenerTest {
    private CommitPolicy policy;
    private Consumer<Object, Object> consumer;
    private RebalanceListener<Object, Object> listener;
    private ProcessRecordsProgress progress;

    @Before
    public void setUp() {
        policy = mock(CommitPolicy.class);
        consumer = mock(Consumer.class);
        progress = new ProcessRecordsProgress();
    }

    @After
    public void tearDown() {
        consumer.close();
    }

    @Test
    public void testPauseNotFinishedPartitionsOnPartitionAssign() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.NONE);
        final List<TopicPartition> pausedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<TopicPartition> partitionToResumeAfterCommit = toPartitions(IntStream.range(0, 20).boxed().collect(toList()));
        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(10, 25).boxed().collect(toList()));
        final List<TopicPartition> partitionStillNeedsToPause = toPartitions(IntStream.range(20, 25).boxed().collect(toList()));

        when(consumer.paused()).thenReturn(new HashSet<>(pausedPartitions));
        when(policy.partialCommitSync(progress)).thenReturn(new HashSet<>(partitionToResumeAfterCommit));

        listener.onPartitionsRevoked(pausedPartitions);
        listener.onPartitionsAssigned(assignedPartitions);

        verify(consumer, times(1)).pause(new HashSet<>(partitionStillNeedsToPause));
    }

    @Test
    public void testPausedPartitionsWasAllRevoked() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.NONE);
        final List<TopicPartition> pausedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<TopicPartition> partitionToResumeAfterCommit = toPartitions(IntStream.range(0, 20).boxed().collect(toList()));
        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(10, 20).boxed().collect(toList()));

        when(consumer.paused()).thenReturn(new HashSet<>(pausedPartitions));
        when(policy.partialCommitSync(progress)).thenReturn(new HashSet<>(partitionToResumeAfterCommit));

        listener.onPartitionsRevoked(pausedPartitions);
        listener.onPartitionsAssigned(assignedPartitions);

        verify(consumer, never()).pause(any());
    }

    @Test
    public void testRevokePartitions() {
        final Set<TopicPartition> allPartitions = new HashSet<>(toPartitions(IntStream.range(0, 30).boxed().collect(toList())));
        final Set<TopicPartition> reAssignedPartitions = new HashSet<>(toPartitions(IntStream.range(0, 20).boxed().collect(toList())));
        final Set<TopicPartition> revokedPartitions = new HashSet<>(toPartitions(IntStream.range(20, 30).boxed().collect(toList())));
        final ProcessRecordsProgress progress = mock(ProcessRecordsProgress.class);
        when(progress.allPartitions()).thenReturn(allPartitions);
        when(consumer.assignment()).thenReturn(reAssignedPartitions);
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.NONE);

        listener.onPartitionsAssigned(reAssignedPartitions);

        verify(progress, times(1)).clearFor(revokedPartitions);
    }

    @Test
    public void testPauseResumeCommit() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.NONE);
        final List<TopicPartition> pausedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        final List<TopicPartition> partitionToResumeAfterCommit = toPartitions(IntStream.range(0, 20).boxed().collect(toList()));
        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(10, 25).boxed().collect(toList()));
        final List<TopicPartition> partitionStillNeedsToPause = toPartitions(IntStream.range(20, 25).boxed().collect(toList()));

        when(consumer.paused()).thenReturn(new HashSet<>(pausedPartitions));
        when(policy.partialCommitSync(progress))
                // Return empty Set on the first call
                .thenReturn(Collections.emptySet())
                // Return resumed partitions on the second call
                .thenReturn(new HashSet<>(partitionToResumeAfterCommit));

        listener.onPartitionsRevoked(pausedPartitions);
        verify(policy, times(1)).pauseCommit();
        listener.onPartitionsAssigned(assignedPartitions);
        verify(policy, times(1)).resumeCommit();

        verify(consumer, times(1)).pause(new HashSet<>(partitionStillNeedsToPause));
    }

    @Test
    public void testForceSeekToBeginningForAllPartitions() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.BEGINNING);

        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        listener.onPartitionsAssigned(assignedPartitions);

        verify(consumer, times(1)).seekToBeginning(argThat(partitions ->
                new HashSet<>(assignedPartitions).equals(new HashSet<>(partitions))
        ));
    }

    @Test
    public void testForceSeekToBeginningForNewPartitions() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.BEGINNING);

        final List<TopicPartition> initialPartitions = toPartitions(IntStream.range(0, 15).boxed().collect(toList()));
        final List<TopicPartition> newAssignedPartitions = toPartitions(IntStream.range(10, 30).boxed().collect(toList()));

        listener.onPartitionsAssigned(initialPartitions);
        listener.onPartitionsAssigned(newAssignedPartitions);

        ArgumentCaptor<Collection<TopicPartition>> seekToBeginningArgs = ArgumentCaptor.forClass(Collection.class);

        verify(consumer, times(2)).seekToBeginning(seekToBeginningArgs.capture());

        assertThat(seekToBeginningArgs.getAllValues().get(0)).containsExactlyInAnyOrderElementsOf(initialPartitions);

        newAssignedPartitions.removeAll(initialPartitions);
        assertThat(seekToBeginningArgs.getAllValues().get(1)).containsExactlyInAnyOrderElementsOf(newAssignedPartitions);
    }

    @Test
    public void testForceSeekToBeginningWithoutNewPartitions() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.BEGINNING);

        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        listener.onPartitionsAssigned(assignedPartitions);
        listener.onPartitionsAssigned(assignedPartitions);
        listener.onPartitionsAssigned(assignedPartitions);

        verify(consumer, times(1)).seekToBeginning(argThat(partitions ->
                new HashSet<>(assignedPartitions).equals(new HashSet<>(partitions))
        ));
    }

    @Test
    public void testForceSeekToEndForAllPartitions() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.END);

        final List<TopicPartition> assignedPartitions = toPartitions(IntStream.range(0, 30).boxed().collect(toList()));
        listener.onPartitionsAssigned(assignedPartitions);

        verify(consumer, times(1)).seekToEnd(argThat(partitions ->
                new HashSet<>(assignedPartitions).equals(new HashSet<>(partitions))
        ));
    }

    @Test
    public void testForceSeekToEndForNewPartitions() {
        listener = new RebalanceListener<>(consumer, progress, policy, ConsumerSeekDestination.END);

        final List<TopicPartition> initialPartitions = toPartitions(IntStream.range(0, 15).boxed().collect(toList()));
        final List<TopicPartition> newAssignedPartitions = toPartitions(IntStream.range(10, 30).boxed().collect(toList()));

        listener.onPartitionsAssigned(initialPartitions);
        listener.onPartitionsAssigned(newAssignedPartitions);

        ArgumentCaptor<Collection<TopicPartition>> seekToEndArgs = ArgumentCaptor.forClass(Collection.class);

        verify(consumer, times(2)).seekToEnd(seekToEndArgs.capture());

        assertThat(seekToEndArgs.getAllValues().get(0)).containsExactlyInAnyOrderElementsOf(initialPartitions);

        newAssignedPartitions.removeAll(initialPartitions);
        assertThat(seekToEndArgs.getAllValues().get(1)).containsExactlyInAnyOrderElementsOf(newAssignedPartitions);
    }
}