package cn.leancloud.kafka.consumer;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

public class RetriableConsumerRecordHandlerTest {
    private static final String topic = "topic";
    private static final ConsumerRecord<Object, Object> testingRecord = new ConsumerRecord<>(topic, 0, 1, new Object(), new Object());
    private ConsumerRecordHandler<Object, Object> wrappedHandler;

    @Before
    public void setUp() {
        wrappedHandler = mock(ConsumerRecordHandler.class);
    }

    @Test
    public void testInvalidRetryTimes() {
        final int retryTimes = -1 * ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
        assertThatThrownBy(() -> new RetriableConsumerRecordHandler<>(wrappedHandler, retryTimes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRetryTimes");
    }

    @Test
    public void testRetry() {
        final Exception expectedEx = new RuntimeException();
        final int retryTimes = 9;

        doThrow(expectedEx).when(wrappedHandler).handleRecord(testingRecord);

        final ConsumerRecordHandler<Object, Object> handler = new RetriableConsumerRecordHandler<>(wrappedHandler, retryTimes);

        assertThatThrownBy(() -> handler.handleRecord(testingRecord))
                .isInstanceOf(HandleMessageFailedException.class)
                .hasCause(expectedEx);

        verify(wrappedHandler, times(retryTimes + 1)).handleRecord(testingRecord);
    }

    @Test
    public void testNoRetry() {
        final int retryTimes = 10;

        doNothing().when(wrappedHandler).handleRecord(testingRecord);

        final ConsumerRecordHandler<Object, Object> handler = new RetriableConsumerRecordHandler<>(wrappedHandler, retryTimes);

        handler.handleRecord(testingRecord);

        verify(wrappedHandler, times(1)).handleRecord(testingRecord);
    }

    @Test
    public void testRetryToSuccess() {
        final Exception expectedEx = new RuntimeException("intended exception");
        final int retryTimes = 10;

        doThrow(expectedEx).doNothing().when(wrappedHandler).handleRecord(testingRecord);

        final ConsumerRecordHandler<Object, Object> handler = new RetriableConsumerRecordHandler<>(wrappedHandler, retryTimes);

        handler.handleRecord(testingRecord);

        verify(wrappedHandler, times(2)).handleRecord(testingRecord);
    }

    @Test
    public void testInterruptInRetryIntervalAndFailForever() throws Exception {
        final Exception expectedEx = new RuntimeException();
        final int retryTimes = 10;

        doThrow(expectedEx).when(wrappedHandler).handleRecord(testingRecord);

        final ConsumerRecordHandler<Object, Object> handler = new RetriableConsumerRecordHandler<>(wrappedHandler, retryTimes, Duration.ofHours(1));
        interruptAfter(Thread.currentThread(), 100);
        assertThatThrownBy(() -> handler.handleRecord(testingRecord))
                .isInstanceOf(HandleMessageFailedException.class)
                .hasCause(expectedEx);

        assertThat(Thread.interrupted()).isTrue();
        verify(wrappedHandler, times(retryTimes + 1)).handleRecord(testingRecord);
    }

    @Test
    public void testInterruptInRetryIntervalAndRetryToSuccess() throws Exception {
        final Exception expectedEx = new RuntimeException("intended exception");
        final int retryTimes = 2;

        doThrow(expectedEx).doThrow(expectedEx).doNothing().when(wrappedHandler).handleRecord(testingRecord);

        final ConsumerRecordHandler<Object, Object> handler = new RetriableConsumerRecordHandler<>(wrappedHandler, retryTimes, Duration.ofHours(1));
        interruptAfter(Thread.currentThread(), 200);
        handler.handleRecord(testingRecord);

        assertThat(Thread.interrupted()).isTrue();
        verify(wrappedHandler, times(3)).handleRecord(testingRecord);
    }

    private void interruptAfter(Thread threadToInterrupt, long triggerDelayMillis) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final Thread thread = new Thread(() -> {
            latch.countDown();
            try {
                Thread.sleep(triggerDelayMillis);
                threadToInterrupt.interrupt();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        thread.start();
        latch.await();
    }
}