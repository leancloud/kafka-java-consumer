package cn.leancloud.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class RetriableMessageHandlerTest {
    private static final String topic = "topic";
    private static final ConsumerRecord<Object, Object> testingRecord = new ConsumerRecord<>(topic, 0, 1, new Object(), new Object());
    private MessageHandler<Object, Object> innerHandler;

    @Before
    public void setUp() {
        innerHandler = mock(MessageHandler.class);
    }

    @Test
    public void testInvalidRetryTimes() {
        final int retryTimes = -1 * ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        assertThatThrownBy(() -> new RetriableMessageHandler<>(innerHandler, retryTimes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetryTimes");
    }

    @Test
    public void testRetry() {
        final Exception expectedEx = new RuntimeException();
        final int retryTimes = 10;

        doThrow(expectedEx).when(innerHandler).handleMessage(testingRecord);

        MessageHandler<Object, Object> handler = new RetriableMessageHandler<>(innerHandler, retryTimes);

        assertThatThrownBy(() -> handler.handleMessage(testingRecord))
                .isInstanceOf(HandleMessageFailedException.class)
                .hasCause(expectedEx);

        verify(innerHandler, times(retryTimes)).handleMessage(testingRecord);
    }

    @Test
    public void testNoRetry() {
        final int retryTimes = 10;

        doNothing().when(innerHandler).handleMessage(testingRecord);

        MessageHandler<Object, Object> handler = new RetriableMessageHandler<>(innerHandler, retryTimes);

        handler.handleMessage(testingRecord);

        verify(innerHandler, times(1)).handleMessage(testingRecord);
    }

    @Test
    public void testRetrySuccess() {
        final Exception expectedEx = new RuntimeException();
        final int retryTimes = 10;

        doThrow(expectedEx).doNothing().when(innerHandler).handleMessage(testingRecord);

        MessageHandler<Object, Object> handler = new RetriableMessageHandler<>(innerHandler, retryTimes);

        handler.handleMessage(testingRecord);

        verify(innerHandler, times(2)).handleMessage(testingRecord);
    }
}