package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Map;

abstract class AbstractPartialCommitPolicy<K, V> extends AbstractRecommitAwareCommitPolicy<K, V> {
    AbstractPartialCommitPolicy(Consumer<K, V> consumer, Duration RecommitInterval) {
        super(consumer, RecommitInterval);
    }

    Map<TopicPartition, OffsetAndMetadata> offsetsForPartialCommit() {
        if (needRecommit()) {
            return offsetsForRecommit();
        } else {
            return completedTopicOffsets;
        }
    }
}
