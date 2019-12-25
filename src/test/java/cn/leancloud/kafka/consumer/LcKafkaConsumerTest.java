package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class LcKafkaConsumerTest {
    private static final List<String> testingTopics = Collections.singletonList("TestingTopic");
    private MessageHandler<Object, Object> messageHandler;
    private Map<String, Object> configs;
    private ExecutorService workerPool;
    private LcKafkaConsumer<Object, Object> consumer;

    @Before
    public void setUp() throws Exception {
        workerPool = mock(ExecutorService.class);
        messageHandler = mock(MessageHandler.class);

        configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
    }

    @Test
    public void testSubscribeWithEmptyTopics() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, messageHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        assertThatThrownBy(() -> consumer.subscribe(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("subscribe empty topics");
    }

    @Test
    public void testSubscribe() {
        final MockConsumer<Object, Object> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, messageHandler)
                .mockKafkaConsumer(kafkaConsumer)
                .buildSync();
        consumer.subscribe(testingTopics);

        assertThat(kafkaConsumer.subscription()).containsExactlyElementsOf(testingTopics);
        assertThat(consumer.subscribed()).isTrue();
    }

    @Test
    public void testSubscribedTwice() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, messageHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        consumer.subscribe(testingTopics);
        assertThatThrownBy(() -> consumer.subscribe(testingTopics))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("client is in SUBSCRIBED state. expect: INIT");
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, messageHandler)
                .workerPool(workerPool, true)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        consumer.subscribe(testingTopics);

        // subscribe topic to change state
        assertThat(consumer.subscribed()).isTrue();
        consumer.close();

        verify(workerPool, times(1)).shutdown();
        verify(workerPool, times(1)).awaitTermination(anyLong(), any());
        assertThat(consumer.closed()).isTrue();
    }

    @Test
    public void testGracefulShutdownWithoutShutdownWorkerPool() throws Exception {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, messageHandler)
                .workerPool(workerPool, false)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        consumer.subscribe(testingTopics);

        // subscribe topic to change state
        assertThat(consumer.subscribed()).isTrue();
        consumer.close();

        verify(workerPool, never()).shutdown();
        verify(workerPool, never()).awaitTermination(anyLong(), any());
        assertThat(consumer.closed()).isTrue();
    }

}