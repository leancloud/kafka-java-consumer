package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

public final class SafetyNetMessageHandler<K, V> implements MessageHandler<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(SafetyNetMessageHandler.class);

    private final MessageHandler<K, V> innerHandler;
    private final BiConsumer<ConsumerRecord<K, V>, Throwable> errorConsumer;

    public SafetyNetMessageHandler(MessageHandler<K, V> innerHandler) {
        this(innerHandler, (record, throwable) -> logger.error("Handle kafka consumer record: " + record + " failed.", throwable));
    }

    public SafetyNetMessageHandler(MessageHandler<K, V> innerHandler, BiConsumer<ConsumerRecord<K, V>, Throwable> errorConsumer) {
        this.innerHandler = innerHandler;
        this.errorConsumer = errorConsumer;
    }

    @Override
    public void handleMessage(ConsumerRecord<K, V> record) {
        try {
            innerHandler.handleMessage(record);
        } catch (Exception ex) {
            errorConsumer.accept(record, ex);
        }
    }
}
