package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class RetriableMessageHandler<K, V> implements MessageHandler<K, V> {
    private final int maxRetryTimes;
    private final MessageHandler<K, V> innerHandler;

    public RetriableMessageHandler(MessageHandler<K, V> innerHandler, int maxRetryTimes) {
        if (maxRetryTimes <= 0) {
            throw new IllegalArgumentException("maxRetryTimes: " + maxRetryTimes + " (expect > 0)");
        }

        this.maxRetryTimes = maxRetryTimes;
        this.innerHandler = innerHandler;
    }

    @Override
    public void handleMessage(ConsumerRecord<K, V> record) {
        Exception lastException = null;
        for (int retried = 0; retried < maxRetryTimes; ++retried) {
            try {
                innerHandler.handleMessage(record);
                lastException = null;
                break;
            } catch (Exception ex) {
                lastException = ex;
            }
        }

        if (lastException != null) {
            throw new HandleMessageFailedException(lastException);
        }
    }
}
