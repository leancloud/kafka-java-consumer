package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;

public class SafetyNetMessageHandlerTest {
    private static final String topic = "topic";
    private static final ConsumerRecord<Object, Object> testingRecord = new ConsumerRecord<>(topic, 0, 1, new Object(), new Object());
    private MessageHandler<Object, Object> innerHandler;

    @Before
    public void setUp() {
        innerHandler = mock(MessageHandler.class);
    }

    @Test
    public void testCustomErrorHandler() {
        final Exception expectedEx = new RuntimeException();
        final BiConsumer<ConsumerRecord<Object, Object>, Throwable> errorConsumer = mock(BiConsumer.class);

        doThrow(expectedEx).when(innerHandler).handleMessage(testingRecord);

        MessageHandler<Object, Object> handler = new SafetyNetMessageHandler<>(innerHandler, errorConsumer);

        handler.handleMessage(testingRecord);

        verify(innerHandler, times(1)).handleMessage(testingRecord);
        verify(errorConsumer, times(1)).accept(testingRecord, expectedEx);
    }

    @Test
    public void testDefaultErrorHandler() {
        final Exception expectedEx = new RuntimeException("expected exception");

        doThrow(expectedEx).when(innerHandler).handleMessage(testingRecord);

        MessageHandler<Object, Object> handler = new SafetyNetMessageHandler<>(innerHandler);

        handler.handleMessage(testingRecord);

        verify(innerHandler, times(1)).handleMessage(testingRecord);
    }

    @Test
    public void testHandleSuccess() {
        final BiConsumer<ConsumerRecord<Object, Object>, Throwable> errorConsumer = mock(BiConsumer.class);

        doNothing().when(innerHandler).handleMessage(testingRecord);

        MessageHandler<Object, Object> handler = new SafetyNetMessageHandler<>(innerHandler, errorConsumer);

        handler.handleMessage(testingRecord);

        verify(innerHandler, times(1)).handleMessage(testingRecord);
        verify(errorConsumer, never()).accept(any(), any());
    }
}