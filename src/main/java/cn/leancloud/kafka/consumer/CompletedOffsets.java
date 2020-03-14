package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.PriorityQueue;

class CompletedOffsets {
    private final PriorityQueue<Long> outOfOrderQueue;
    private long completedOffset;
    private long nextOffsetToCommit;

    CompletedOffsets(long lastCompletedOffset) {
        this.completedOffset = lastCompletedOffset;
        this.nextOffsetToCommit = lastCompletedOffset + 1;
        this.outOfOrderQueue = new PriorityQueue<>();
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
        return completedOffset < nextOffsetToCommit - 1;
    }

    OffsetAndMetadata getOffsetToCommit() {
        assert hasOffsetToCommit();
        return new OffsetAndMetadata(nextOffsetToCommit);
    }

    /**
     * Update committed offset, completed offset = committed offset - 1
     * @param committedOffset the offset that committed successfully
     */
    void updateCommittedOffset(long committedOffset) {
        assert committedOffset > this.completedOffset : "old:" + this.completedOffset + " new:" + committedOffset;
        assert committedOffset <= nextOffsetToCommit : "completedOffset:" + committedOffset + " nextOffsetToCommit:" + nextOffsetToCommit;
        this.completedOffset = committedOffset - 1;
    }
}
