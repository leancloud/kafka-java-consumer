package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class CatchAllExceptionConsumerRecordHandlerTest {
    private static final String topic = "topic";
    private static final ConsumerRecord<Object, Object> testingRecord = new ConsumerRecord<>(topic, 0, 1, new Object(), new Object());
    private ConsumerRecordHandler<Object, Object> wrappedHandler;

    @Before
    public void setUp() {
        wrappedHandler = mock(ConsumerRecordHandler.class);
    }

    @Test
    public void testNullWrappedHandler() {
        assertThatThrownBy(() -> new CatchAllExceptionConsumerRecordHandler<>(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("wrappedHandler");

        assertThatThrownBy(() -> new CatchAllExceptionConsumerRecordHandler<>(null, ((record, throwable) -> {})))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("wrappedHandler");
    }

    @Test
    public void testCustomErrorHandler() {
        final Exception expectedEx = new RuntimeException();
        final BiConsumer<ConsumerRecord<Object, Object>, Throwable> errorConsumer = mock(BiConsumer.class);

        doThrow(expectedEx).when(wrappedHandler).handleRecord(testingRecord);

        ConsumerRecordHandler<Object, Object> handler = new CatchAllExceptionConsumerRecordHandler<>(wrappedHandler, errorConsumer);

        handler.handleRecord(testingRecord);

        verify(wrappedHandler, times(1)).handleRecord(testingRecord);
        verify(errorConsumer, times(1)).accept(testingRecord, expectedEx);
    }

    @Test
    public void testDefaultErrorHandler() {
        final Exception expectedEx = new RuntimeException("expected exception");

        doThrow(expectedEx).when(wrappedHandler).handleRecord(testingRecord);

        ConsumerRecordHandler<Object, Object> handler = new CatchAllExceptionConsumerRecordHandler<>(wrappedHandler);

        handler.handleRecord(testingRecord);

        verify(wrappedHandler, times(1)).handleRecord(testingRecord);
    }

    @Test
    public void testHandleSuccess() {
        final BiConsumer<ConsumerRecord<Object, Object>, Throwable> errorConsumer = mock(BiConsumer.class);

        doNothing().when(wrappedHandler).handleRecord(testingRecord);

        ConsumerRecordHandler<Object, Object> handler = new CatchAllExceptionConsumerRecordHandler<>(wrappedHandler, errorConsumer);

        handler.handleRecord(testingRecord);

        verify(wrappedHandler, times(1)).handleRecord(testingRecord);
        verify(errorConsumer, never()).accept(any(), any());
    }
}