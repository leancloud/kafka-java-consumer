package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * The destination for a consumer reset offset operation.
 */
public enum ConsumerSeekDestination {
    /**
     * Do nothing. Do not reset offset of the given consumer to anywhere.
     */
    NONE {
        @Override
        public void seek(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {

        }
    },
    /**
     * Reset offsets for all the given partitions of a given consumer to beginning.
     */
    BEGINNING {
        @Override
        public void seek(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            consumer.seekToBeginning(partitions);
        }
    },
    /**
     * Reset offsets for all the given partitions of a given consumer to end.
     */
    END {
        @Override
        public void seek(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
            consumer.seekToEnd(partitions);
        }
    };

    /**
     * Reset offsets for all the given partitions of a given consumer.
     *
     * @param consumer   the consumer to reset offset
     * @param partitions the partitions to reset offsets
     */
    abstract public void seek(Consumer<?, ?> consumer, Collection<TopicPartition> partitions);
}
