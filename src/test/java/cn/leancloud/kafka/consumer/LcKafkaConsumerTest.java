package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import static cn.leancloud.kafka.consumer.TestingUtils.testingTopic;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class LcKafkaConsumerTest {
    private static final List<String> testingTopics = Collections.singletonList("TestingTopic");
    private ConsumerRecordHandler<Object, Object> consumerRecordHandler;
    private Map<String, Object> configs;
    private ExecutorService workerPool;
    private LcKafkaConsumer<Object, Object> consumer;

    @Before
    public void setUp() throws Exception {
        workerPool = mock(ExecutorService.class);
        consumerRecordHandler = mock(ConsumerRecordHandler.class);

        configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("max.poll.records", "10");
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
    }

    @Test
    public void testSubscribeNullTopics() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        Collection<String> topics = null;
        assertThatThrownBy(() -> consumer.subscribe(topics))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("topics");
    }

    @Test
    public void testSubscribeWithEmptyTopics() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        assertThatThrownBy(() -> consumer.subscribe(Collections.emptyList()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("subscribe empty topics");
    }

    @Test
    public void testSubscribeContainsEmptyTopics() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        assertThatThrownBy(() -> consumer.subscribe(Arrays.asList("Topic", "")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("topic collection to subscribe to cannot contain null or empty topic");
    }

    @Test
    public void testSubscribeContainsNullPattern() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        Pattern pattern = null;
        assertThatThrownBy(() -> consumer.subscribe(pattern))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pattern");
    }

    @Test
    public void testSubscribeNull() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        assertThatThrownBy(() -> consumer.subscribe(Arrays.asList("Topic", null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("topic collection to subscribe to cannot contain null or empty topic");
    }

    @Test
    public void testSubscribeTopics() {
        final MockConsumer<Object, Object> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(kafkaConsumer)
                .buildSync();
        consumer.subscribe(testingTopics);

        assertThat(kafkaConsumer.subscription()).containsExactlyElementsOf(testingTopics);
        assertThat(consumer.subscribed()).isTrue();
    }

    @Test
    public void testSubscribePattern() {
        final MockConsumer<Object, Object> kafkaConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(kafkaConsumer)
                .buildSync();

        final List<Integer> partitions = IntStream.range(0, 30).boxed().collect(toList());
        kafkaConsumer.updateEndOffsets(generateEndOffsets(partitions, 0));
        kafkaConsumer.updatePartitions(testingTopic, generatePartitionInfos(partitions));
        final Pattern pattern = Pattern.compile("Test.*");
        consumer.subscribe(pattern);

        assertThat(kafkaConsumer.subscription()).containsExactlyElementsOf(testingTopics);
        assertThat(consumer.subscribed()).isTrue();
    }

    @Test
    public void testSubscribedTwice() {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .recommitInterval(Duration.ofDays(1))
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .buildSync();
        consumer.subscribe(testingTopics);
        assertThatThrownBy(() -> consumer.subscribe(testingTopics))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("consumer is closed or have subscribed to some topics or pattern");
    }

    @Test
    public void testGracefulShutdown() throws Exception {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .workerPool(workerPool, true)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .recommitInterval(Duration.ofDays(1))
                .buildSync();
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = consumer.subscribe(testingTopics);

        // subscribe topic to change state
        assertThat(consumer.subscribed()).isTrue();
        consumer.close();

        verify(workerPool, times(1)).shutdown();
        verify(workerPool, times(1)).awaitTermination(anyLong(), any());
        assertThat(consumer.closed()).isTrue();
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.CLOSED);
    }

    @Test
    public void testGracefulShutdownWithoutShutdownWorkerPool() throws Exception {
        consumer = LcKafkaConsumerBuilder.newBuilder(configs, consumerRecordHandler)
                .workerPool(workerPool, false)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .recommitInterval(Duration.ofDays(1))
                .buildSync();
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = consumer.subscribe(testingTopics);

        // subscribe topic to change state
        assertThat(consumer.subscribed()).isTrue();
        consumer.close();

        verify(workerPool, never()).shutdown();
        verify(workerPool, never()).awaitTermination(anyLong(), any());
        assertThat(consumer.closed()).isTrue();
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.CLOSED);
    }

    @Test
    public void testShutdownOnUnsubscribe() throws Exception {
        final MockConsumer<Object, Object> mockedConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        // setup an exception
        mockedConsumer.setException(new KafkaException("Testing shutdown fetcher exception"));
        consumer = LcKafkaConsumerBuilder
                .newBuilder(configs, consumerRecordHandler)
                .workerPool(workerPool, true)
                .mockKafkaConsumer(mockedConsumer)
                .recommitInterval(Duration.ofDays(1))
                .buildSync();
        final CompletableFuture<UnsubscribedStatus> unsubscribedStatusFuture = consumer.subscribe(testingTopics);

        // subscribe topic to change state
        assertThat(consumer.subscribed()).isTrue();

        await().until(() -> consumer.closed());

        verify(workerPool, times(1)).shutdown();
        verify(workerPool, times(1)).awaitTermination(anyLong(), any());
        assertThat(consumer.closed()).isTrue();
        assertThat(unsubscribedStatusFuture).isCompletedWithValue(UnsubscribedStatus.ERROR);
    }

    private List<PartitionInfo> generatePartitionInfos(List<Integer> partitions) {
        return partitions
                .stream()
                .map(p -> new PartitionInfo(testingTopic, p, null, null, null))
                .collect(toList());
    }

    private Map<TopicPartition, Long> generateEndOffsets(List<Integer> partitions, long endOffset) {
        return partitions
                .stream()
                .map(p -> new TopicPartition(testingTopic, p))
                .collect(toMap(Function.identity(), (p) -> endOffset));


    }
}