package cn.leancloud.kafka.consumer;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

import static java.util.Collections.emptySet;

final class NoOpCommitPolicy implements CommitPolicy {
    private static final NoOpCommitPolicy INSTANCE = new NoOpCommitPolicy();

    static NoOpCommitPolicy getInstance() {
        return INSTANCE;
    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords, ProcessRecordsProgress progress) {
        return emptySet();
    }

    @Override
    public Set<TopicPartition> partialCommitSync(ProcessRecordsProgress progress) {
        return emptySet();
    }
}
