package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.PriorityQueue;

class CompletedOffsets {
    private final PriorityQueue<Long> outOfOrderQueue;
    private long committedOffset;
    private long nextOffsetToCommit;

    CompletedOffsets(long lastCommittedOffset) {
        this.committedOffset = lastCommittedOffset;
        this.nextOffsetToCommit = lastCommittedOffset + 1;
        this.outOfOrderQueue = new PriorityQueue<>();
    }

    long nextOffsetToCommit() {
        return nextOffsetToCommit;
    }

    void addCompleteOffset(long offset) {
        if (offset == nextOffsetToCommit) {
            ++nextOffsetToCommit;

            while (!outOfOrderQueue.isEmpty() && outOfOrderQueue.peek() == nextOffsetToCommit) {
                outOfOrderQueue.poll();
                ++nextOffsetToCommit;
            }
        } else if (offset > nextOffsetToCommit) {
            outOfOrderQueue.add(offset);
        }
    }

    boolean hasOffsetToCommit() {
        return committedOffset < nextOffsetToCommit - 1;
    }

    OffsetAndMetadata getOffsetToCommit() {
        return new OffsetAndMetadata(nextOffsetToCommit);
    }

    void updateCommittedOffset(long committedOffset) {
        assert committedOffset > this.committedOffset : "old:" + this.committedOffset + " new:" + committedOffset;
        this.committedOffset = committedOffset;
    }
}
