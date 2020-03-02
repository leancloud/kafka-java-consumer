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

import static cn.leancloud.kafka.consumer.AutoCommitConsumerConfigs.AUTO_COMMIT_INTERVAL_MS;
import static cn.leancloud.kafka.consumer.AutoCommitConsumerConfigs.MAX_POLL_INTERVAL_MS;
import static cn.leancloud.kafka.consumer.BasicConsumerConfigs.AUTO_OFFSET_RESET;
import static cn.leancloud.kafka.consumer.BasicConsumerConfigs.MAX_POLL_RECORDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class LcKafkaConsumerBuilderTest {
    private ExecutorService workerPool;
    private Map<String, Object> configs;
    private ConsumerRecordHandler<Object, Object> testingHandler;
    private Deserializer<Object> keyDeserializer;
    private Deserializer<Object> valueDeserializer;

    @Before
    public void setUp() throws Exception {
        workerPool = Executors.newCachedThreadPool(new NamedThreadFactory("Testing-Pool"));
        configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("group.id", "2614911922612339122");

        testingHandler = mock(ConsumerRecordHandler.class);
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
    public void testNegativePollTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .pollTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pollTimeout: PT-1S (expect positive or zero duration)");
    }

    @Test
    public void testNegativeHandleRecordTimeoutMs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .handleRecordTimeoutMillis(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("handleRecordTimeoutMillis");
    }

    @Test
    public void testNullHandleRecordTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .handleRecordTimeout(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("handleRecordTimeout");
    }

    @Test
    public void testNegativeHandleRecordTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .handleRecordTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("handleRecordTimeout: PT-1S (expect positive or zero duration)");
    }

    @Test
    public void testNullForceSeekDestination() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .forceSeekTo(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("destination");
    }

    @Test
    public void testNegativeShutdownTimeoutMs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .gracefulShutdownTimeoutMillis(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("gracefulShutdownTimeoutMillis");
    }

    @Test
    public void testNullShutdownTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .gracefulShutdownTimeout(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("gracefulShutdownTimeout");
    }

    @Test
    public void testNegativeShutdownTimeout() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .gracefulShutdownTimeout(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("gracefulShutdownTimeout: PT-1S (expect positive or zero duration)");
    }

    @Test
    public void testNegativeSyncCommitRetryIntervalMs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .syncCommitRetryIntervalMillis(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("syncCommitRetryIntervalMillis");
    }

    @Test
    public void testNullSyncCommitRetryInterval() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .syncCommitRetryInterval(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("syncCommitRetryInterval");
    }

    @Test
    public void testNegativeSyncCommitRetryInterval() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .syncCommitRetryInterval(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("syncCommitRetryInterval: PT-1S (expect positive or zero duration)");
    }

    @Test
    public void testNegativeMaxAttemptsForEachSyncCommit() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .maxAttemptsForEachSyncCommit(-1 * ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttemptsForEachSyncCommit");
    }

    @Test
    public void testZeroMaxAttemptsForEachSyncCommit() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .maxAttemptsForEachSyncCommit(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttemptsForEachSyncCommit");
    }

    @Test
    public void testNegativeRecommitIntervalMs() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .recommitIntervalInMillis(-1 * ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recommitIntervalInMillis");
    }

    @Test
    public void testNullRecommitInterval() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .recommitInterval(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("recommitInterval");
    }

    @Test
    public void testNegativeRecommitInterval() {
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .recommitInterval(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("recommitInterval: PT-1S (expect positive duration)");
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
        MAX_POLL_RECORDS.set(configs, 10);
        AUTO_COMMIT_INTERVAL_MS.set(configs, 1000);
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"max.poll.interval.ms\"");
    }

    @Test
    public void testAutoConsumerWithoutAutoCommitInterval() {
        MAX_POLL_RECORDS.set(configs, 10);
        MAX_POLL_INTERVAL_MS.set(configs, 1000);
        assertThatThrownBy(() -> LcKafkaConsumerBuilder.newBuilder(configs, testingHandler, keyDeserializer, valueDeserializer)
                .buildAuto())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expect \"auto.commit.interval.ms\"");
    }

    @Test
    public void testAutoConsumer() {
        MAX_POLL_RECORDS.set(configs, 10);
        MAX_POLL_INTERVAL_MS.set(configs, 1000);
        AUTO_COMMIT_INTERVAL_MS.set(configs, 1000);
        AUTO_OFFSET_RESET.set(configs, "latest");
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeoutMillis(1000)
                .maxPendingAsyncCommits(100)
                .buildAuto();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(NoOpCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testSyncConsumer() {
        AUTO_OFFSET_RESET.set(configs, "latest");
        MAX_POLL_RECORDS.set(configs, 10);
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .forceSeekTo(ConsumerSeekDestination.END)
                .maxPendingAsyncCommits(100)
                .workerPool(workerPool, false)
                .recommitInterval(Duration.ofMinutes(20))
                .syncCommitRetryIntervalMillis(2000)
                .maxAttemptsForEachSyncCommit(10)
                .buildSync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(SyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testSyncWithoutWorkerPoolConsumer() {
        AUTO_OFFSET_RESET.set(configs, "latest");
        MAX_POLL_RECORDS.set(configs, 10);
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .recommitInterval(Duration.ofMinutes(20))
                .syncCommitRetryInterval(Duration.ofSeconds(2))
                .handleRecordTimeout(Duration.ZERO)
                .buildSync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(SyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testAsyncConsumer() {
        AUTO_OFFSET_RESET.set(configs, "latest");
        MAX_POLL_RECORDS.set(configs, 10);
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .gracefulShutdownTimeoutMillis(15_000)
                .handleRecordTimeoutMillis(10_000)
                .workerPool(workerPool, false)
                .recommitInterval(Duration.ofMinutes(20))
                .buildAsync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(AsyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testPartialSyncConsumer() {
        AUTO_OFFSET_RESET.set(configs, "latest");
        MAX_POLL_RECORDS.set(configs, 10);
        final LcKafkaConsumer<Object, Object> consumer = LcKafkaConsumerBuilder.newBuilder(configs, testingHandler)
                .mockKafkaConsumer(new MockConsumer<>(OffsetResetStrategy.LATEST))
                .pollTimeout(Duration.ofMillis(1000))
                .maxPendingAsyncCommits(100)
                .recommitIntervalInMillis(100_000)
                .gracefulShutdownTimeout(Duration.ofDays(1))
                .workerPool(workerPool, false)
                .buildPartialSync();

        assertThat(consumer).isNotNull();
        assertThat(consumer.policy()).isInstanceOf(PartialSyncCommitPolicy.class);
        consumer.close();
    }

    @Test
    public void testPartialAsyncConsumer() {
        AUTO_OFFSET_RESET.set(configs, "latest");
        MAX_POLL_RECORDS.set(configs, 10);
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