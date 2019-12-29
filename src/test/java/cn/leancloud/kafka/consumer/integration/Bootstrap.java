package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.ConsumerRecordHandler;
import cn.leancloud.kafka.consumer.LcKafkaConsumer;
import cn.leancloud.kafka.consumer.LcKafkaConsumerBuilder;
import cn.leancloud.kafka.consumer.NamedThreadFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class Bootstrap {
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static final ExecutorService workerPool = Executors.newCachedThreadPool(new NamedThreadFactory("integration-test-pool-"));
    private static final String testingTopic = "Testing";
    private static final String testingConsumerGroupId = "2614911922612339122";
    private static final Map<String, Object> defaultConsumerConfigs;
    private static final Deserializer<Integer> defaultKeyDeserializer;
    private static final Deserializer<String> defaultValueDeserializer;

    static {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("group.id", testingConsumerGroupId);
        configs.put("max.poll.interval.ms", 5000);
        defaultConsumerConfigs = configs;
        defaultKeyDeserializer = new IntegerDeserializer();
        defaultValueDeserializer = new StringDeserializer();
    }

    public static void main(String[] args) throws Exception {
        cleanAllPartitions();

        final List<TestRunner> runners = prepareRunners();

        try {
            for (TestRunner runner : runners) {
                runner.runTest(new SingleConsumerTest());
                runner.runTest(new TwoConsumerTest());
                runner.runTest(new JoinLeaveGroupTest());
            }
        } finally {
            shutdown(runners);
            workerPool.shutdown();
        }
    }

    private static void cleanAllPartitions() {
        Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
        configs.put("client.id", "Clean-Previous-Records");
        final KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(configs,
                defaultKeyDeserializer, defaultValueDeserializer);

        consumer.subscribe(Collections.singletonList(testingTopic));
        consumer.seekToBeginning(consumer.assignment());

        outer: while (true) {
            final ConsumerRecords<Integer, String> records = consumer.poll(1000);
            if (!consumer.assignment().isEmpty() && records.isEmpty()) {
                for (Map.Entry<TopicPartition, Long> offsetsEntry : consumer.endOffsets(consumer.assignment()).entrySet()) {
                    final OffsetAndMetadata committedOffset = consumer.committed(offsetsEntry.getKey());
                    if (committedOffset.offset() != offsetsEntry.getValue()) {
                        continue outer;
                    }
                }

                break;
            }

            consumer.commitSync();
        }

        consumer.close();
    }

    private static List<TestRunner> prepareRunners() {
        logger.info("Preparing test runners");
        final List<TestRunner> runners = Stream.of(
                new AutoCommitWithWorkerPoolConsumerFactory(),
                new AutoCommitWithoutWorkerPoolConsumerFactory(),
                new SyncCommitWithWorkerPoolConsumerFactory(),
                new SyncCommitWithoutWorkerPoolConsumerFactory(),
                new PartialSyncCommitWithWorkerPoolConsumerFactory(),
                new PartialSyncCommitWithoutWorkerPoolConsumerFactory(),
                new AsyncCommitWithWorkerPoolConsumerFactory(),
                new AsyncCommitWithoutWorkerPoolConsumerFactory(),
                new PartialAsyncCommitWithWorkerPoolConsumerFactory(),
                new PartialAsyncCommitWithoutWorkerPoolConsumerFactory())
                .map(factory -> new TestRunner(testingTopic, factory))
                .collect(toList());
        logger.info("Test runners prepared\n");
        return runners;
    }

    private static void shutdown(List<TestRunner> runners) throws Exception {
        for (TestRunner runner : runners) {
            runner.close();
        }
    }

    private static class AutoCommitWithWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Auto-Commit-With-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);
            configs.put("auto.commit.interval.ms", 5000);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildAuto();
        }
    }

    private static class AutoCommitWithoutWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Auto-Commit-Without-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);
            configs.put("auto.commit.interval.ms", 5000);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .buildAuto();
        }
    }

    private static class SyncCommitWithWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Sync-Commit-With-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildSync();
        }
    }

    private static class SyncCommitWithoutWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Sync-Commit-Without-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .buildSync();
        }
    }

    private static class PartialSyncCommitWithWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Partial-Sync-Commit-With-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .forceWholeCommitInterval(Duration.ofSeconds(1))
                    .buildPartialSync();
        }
    }

    private static class PartialSyncCommitWithoutWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Partial-Sync-Commit-Without-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .forceWholeCommitInterval(Duration.ofSeconds(1))
                    .buildPartialSync();
        }
    }

    private static class AsyncCommitWithWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Async-Commit-With-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildAsync();
        }
    }

    private static class AsyncCommitWithoutWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Async-Commit-Without-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .buildAsync();
        }
    }

    private static class PartialAsyncCommitWithWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Partial-Async-Commit-With-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .forceWholeCommitInterval(Duration.ofSeconds(1))
                    .buildAsync();
        }
    }

    private static class PartialAsyncCommitWithoutWorkerPoolConsumerFactory implements ConsumerFactory {
        @Override
        public String type() {
            return "Partial-Async-Commit-Without-Worker-Pool-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final ConsumerRecordHandler<Integer, String> handler = createRecordHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>(defaultConsumerConfigs);
            configs.put("client.id", consumerName);
            configs.put("max.poll.records", 2);

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .forceWholeCommitInterval(Duration.ofSeconds(1))
                    .buildAsync();
        }
    }

    private static ConsumerRecordHandler<Integer, String> createRecordHandler(String consumerName, TestStatistics statistics) {
        return (record) -> {
            final String topic = record.topic();
            if (!statistics.recordReceivedRecord(record)) {
                logger.info("{} receive duplicate msg from {} with value: {} on partition: {} at: {}",
                        consumerName, topic, record.value(), record.partition(), record.offset());
            } else {
                logger.debug("{} receive msg from {} with value: {} on partition: {} at: {}",
                        consumerName, topic, record.value(), record.partition(), record.offset());
            }
        };
    }
}
