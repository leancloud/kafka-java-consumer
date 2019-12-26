package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Set;

final class NoOpCommitPolicy<K, V> implements CommitPolicy<K, V> {
    private static final NoOpCommitPolicy INSTANCE = new NoOpCommitPolicy();

    @SuppressWarnings("unchecked")
    static <K, V> NoOpCommitPolicy<K, V> getInstance() {
        return (NoOpCommitPolicy<K, V>) INSTANCE;
    }

    @Override
    public void addPendingRecord(ConsumerRecord<K, V> record) {

    }

    @Override
    public void completeRecord(ConsumerRecord<K, V> record) {

    }

    @Override
    public Set<TopicPartition> tryCommit(boolean noPendingRecords) {
        return Collections.emptySet();
    }

    @Override
    public Set<TopicPartition> partialCommit() {
        return Collections.emptySet();
    }
}
