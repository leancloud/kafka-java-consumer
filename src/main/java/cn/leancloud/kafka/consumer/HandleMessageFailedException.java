package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Thrown by {@link ConsumerRecordHandler)} when the record handler failed to handle a {@link ConsumerRecord}. The origin
 * exception will be wrapped in this exception.
 */
public final class HandleMessageFailedException extends RuntimeException {
    public HandleMessageFailedException() {
        super();
    }

    public HandleMessageFailedException(String message) {
        super(message);
    }

    public HandleMessageFailedException(Throwable throwable) {
        super(throwable);
    }

    public HandleMessageFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}

