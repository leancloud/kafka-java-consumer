package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.BiConsumer;

/**
 * A wrapper over a {@link ConsumerRecordHandler} to catch and swallow all the exceptions throw from the
 * wrapped {@code ConsumerRecordHandler} when it failed to handle a consumed record.
 * <p>
 * This handler seems good to improve the availability of the consumer because it can swallow all the exceptions
 * on handling a record and carry on to handle next record. But it actually can compromise
 * the consumer to prevent a livelock, where the application did not crash but fails to
 * make progress for some reason.
 * Please use it judiciously. Usually fail fast, let the polling thread exit on exception, is your best choice.
 *
 * @param <K> the type of key for records consumed from Kafka
 * @param <V> the type of value for records consumed from Kafka
 */
public final class CatchAllExceptionConsumerRecordHandler<K, V> implements ConsumerRecordHandler<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(CatchAllExceptionConsumerRecordHandler.class);

    private final ConsumerRecordHandler<K, V> wrappedHandler;
    private final BiConsumer<ConsumerRecord<K, V>, Throwable> errorConsumer;

    /**
     * Constructor for {@code CatchAllExceptionConsumerRecordHandler} to just log the failed record when
     * the wrapped handler failed on calling {@link ConsumerRecordHandler#handleRecord(ConsumerRecord)}.
     *
     * @param wrappedHandler the wrapped {@link ConsumerRecordHandler}.
     */
    public CatchAllExceptionConsumerRecordHandler(ConsumerRecordHandler<K, V> wrappedHandler) {
        this(wrappedHandler, (record, throwable) -> logger.error("Handle kafka consumer record: " + record + " failed.", throwable));
    }

    /**
     * Constructor for {@code CatchAllExceptionConsumerRecordHandler} to use a {@link BiConsumer} to handle the
     * failed record when the wrapped handler failed on calling {@link ConsumerRecordHandler#handleRecord(ConsumerRecord)}.
     *
     * @param wrappedHandler the wrapped {@link ConsumerRecordHandler}.
     * @param errorConsumer  a {@link BiConsumer} to consume the failed record and the exception thrown from
     *                       the {@link ConsumerRecordHandler#handleRecord(ConsumerRecord)}
     */
    public CatchAllExceptionConsumerRecordHandler(ConsumerRecordHandler<K, V> wrappedHandler,
                                                  BiConsumer<ConsumerRecord<K, V>, Throwable> errorConsumer) {
        this.wrappedHandler = wrappedHandler;
        this.errorConsumer = errorConsumer;
    }

    @Override
    public void handleRecord(ConsumerRecord<K, V> record) {
        try {
            wrappedHandler.handleRecord(record);
        } catch (Exception ex) {
            errorConsumer.accept(record, ex);
        }
    }
}
