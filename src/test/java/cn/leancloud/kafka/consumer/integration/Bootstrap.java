package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.LcKafkaConsumer;
import cn.leancloud.kafka.consumer.LcKafkaConsumerBuilder;
import cn.leancloud.kafka.consumer.MessageHandler;
import cn.leancloud.kafka.consumer.NamedThreadFactory;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Bootstrap {
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);
    private static final ExecutorService workerPool = Executors.newCachedThreadPool(new NamedThreadFactory("integration-test-pool-"));

    public static void main(String[] args) throws Exception {
        final List<ConsumerFactory> factories = Arrays.asList(
                new SyncCommitConsumerFactory(),
                new PartialSyncCommitConsumerFactory(),
                new AsyncCommitConsumerFactory(),
                new PartialAsyncCommitConsumerFactory());

        for (ConsumerFactory factory : factories) {
            logger.info("Start {} consumer test.", factory.getName());
            try (TestRunner runner = new TestRunner(factory)) {
                runner.startTest();
            }
            logger.info("{} test finished.\n", factory.getName());
        }

        workerPool.shutdown();
    }

    private static class SyncCommitConsumerFactory implements ConsumerFactory {
        @Override
        public String getName() {
            return "Sync-Commit-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final MessageHandler<Integer, String> handler = createMessageHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>();
            configs.put("bootstrap.servers", "localhost:9092");
            configs.put("client.id", consumerName);
            configs.put("auto.offset.reset", "earliest");
            configs.put("group.id", "2614911922612339122");
            configs.put("max.poll.records", 2);
            configs.put("max.poll.interval.ms", "5000");

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildSync();
        }
    }

    private static class PartialSyncCommitConsumerFactory implements ConsumerFactory {
        @Override
        public String getName() {
            return "Partial-Sync-Commit-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final MessageHandler<Integer, String> handler = createMessageHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>();
            configs.put("bootstrap.servers", "localhost:9092");
            configs.put("client.id", consumerName);
            configs.put("auto.offset.reset", "earliest");
            configs.put("group.id", "2614911922612339122");
            configs.put("max.poll.records", 2);
            configs.put("max.poll.interval.ms", "5000");

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildPartialSync();
        }
    }

    private static class AsyncCommitConsumerFactory implements ConsumerFactory {
        @Override
        public String getName() {
            return "Async-Commit-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final MessageHandler<Integer, String> handler = createMessageHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>();
            configs.put("bootstrap.servers", "localhost:9092");
            configs.put("client.id", consumerName);
            configs.put("auto.offset.reset", "earliest");
            configs.put("group.id", "2614911922612339122");
            configs.put("max.poll.records", 2);
            configs.put("max.poll.interval.ms", "5000");

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildAsync();
        }
    }

    private static class PartialAsyncCommitConsumerFactory implements ConsumerFactory {
        @Override
        public String getName() {
            return "Partial-Async-Commit-Consumer";
        }

        @Override
        public LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics) {
            final MessageHandler<Integer, String> handler = createMessageHandler(consumerName, statistics);
            final Map<String, Object> configs = new HashMap<>();
            configs.put("bootstrap.servers", "localhost:9092");
            configs.put("client.id", consumerName);
            configs.put("auto.offset.reset", "earliest");
            configs.put("group.id", "2614911922612339122");
            configs.put("max.poll.records", 2);
            configs.put("max.poll.interval.ms", "5000");

            return LcKafkaConsumerBuilder.newBuilder(
                    configs,
                    handler,
                    new IntegerDeserializer(),
                    new StringDeserializer())
                    .workerPool(workerPool, false)
                    .buildAsync();
        }
    }

    private static MessageHandler<Integer, String> createMessageHandler(String consumerName, TestStatistics statistics) {
        return (record) -> {
            final String topic = record.topic();
            if (!statistics.recordReceivedRecord(record)) {
                logger.info("{} receive duplicate msg from {} with value: {}", consumerName, topic, record.value());
            } else {
                logger.debug("{} receive msg from {} with value: {}", consumerName, topic, record.value());
            }
        };
    }
}
