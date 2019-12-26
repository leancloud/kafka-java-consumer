package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class LcKafkaConsumerBuilderTest {
    private ExecutorService workerPool;
    private Map<String, Object> configs;
    private MessageHandler<Object, Object> testingHandler;
    private Deserializer<Object> keyDeserializer;
    private Deserializer<Object> valueDeserializer;

    @Before
    public void setUp() throws Exception {
        workerPool = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("group.id", "2614911922612339122");

        testingHandler = mock(MessageHandler.class);
        keyDeserializer = mock(Deserializer.class);
        valueDeserializer = mock(Deserializer.class);
    }

    @After
    public void tearDown() throws Exception {
        workerPool.shutdown();
    }

    @Test
    public void testNullKafkaConfigs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(null, testingHandler))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("kafkaConfigs");

        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(null, testingHandler, keyDeserializer, valueDeserializer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("kafkaConfigs");
    }

    @Test
    public void testNullMessageHandler() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageHandler");

        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, null, keyDeserializer, valueDeserializer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageHandler");

        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .messageHandler(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("messageHandler");
    }

    @Test
    public void testNullDeserializers() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, null, valueDeserializer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("keyDeserializer");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("valueDeserializer");
    }

    @Test
    public void testNegativePollTimeoutMs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .pollTimeoutMillis(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pollTimeoutMillis");
    }

    @Test
    public void testNullPollTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .pollTimeout(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("pollTimeout");
    }

    @Test
    public void testNegativeShutdownTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .gracefulShutdownTimeoutMillis(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gracefulShutdownMillis");
    }

    @Test
    public void testNullShutdownTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .gracefulShutdownTimeout(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("gracefulShutdownTimeout");
    }

    @Test
    public void testNegativeMaxPendingAsyncCommits() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .maxPendingAsyncCommits(-1 * ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPendingAsyncCommits");
    }

    @Test
    public void testZeroMaxPendingAsyncCommits() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .maxPendingAsyncCommits(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPendingAsyncCommits");
    }

    @Test
    public void testNullWorkerPool() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .workerPool(null, false))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("workerPool");
    }

    @Test
    public void testAutoConsumerWithoutMaxPollInterval() {
        configs.put("max.poll.records", "10");
        configs.put("auto.commit.interval.ms", "1000");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"max.poll.interval.ms\"");
    }

    @Test
    public void testAutoConsumerWithoutMaxPollRecords() {
        configs.put("max.poll.interval.ms", "1000");
        configs.put("auto.commit.interval.ms", "1000");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"max.poll.records\"");
    }

    @Test
    public void testAutoConsumerWithoutAutoCommitInterval() {
        configs.put("max.poll.records", "10");
        configs.put("max.poll.interval.ms", "1000");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"auto.commit.interval.ms\"");
    }

    @Test
    public void testAutoConsumerWithShouldShutdownWorkerPool() {
        configs.put("max.poll.records", "10");
        configs.put("max.poll.interval.ms", "1000");
        configs.put("auto.commit.interval.ms", "1000");
        configs.put("auto.offset.reset", "latest");
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeoutMillis(1000)
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, true)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("auto commit consumer don't need a worker pool");
    }

    @Test
    public void testAutoConsumer() {
        configs.put("max.poll.records", "10");
        configs.put("max.poll.interval.ms", "1000");
        configs.put("auto.commit.interval.ms", "1000");
        configs.put("auto.offset.reset", "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeoutMillis(1000)
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, false)
                .buildAuto();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(AutoCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testSyncConsumer() {
        configs.put("auto.offset.reset", "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, false)
                .buildSync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(SyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testSyncWithoutWorkerPoolConsumer() {
        configs.put("auto.offset.reset", "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .buildSync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(SyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testASyncConsumer() {
        configs.put("auto.offset.reset", "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, false)
                .buildAsync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(AsyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testPartialSyncConsumer() {
        configs.put("auto.offset.reset", "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, false)
                .buildPartialSync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(PartialSyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testPartialAsyncConsumer() {
        configs.put("auto.offset.reset", "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, false)
                .buildPartialAsync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(PartialAsyncCommitPolicy.class);
        consumer.close();
    }
}