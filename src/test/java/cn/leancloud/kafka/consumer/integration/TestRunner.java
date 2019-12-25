package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.LcKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

public class TestRunner implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

    private final TestingProducer producer;
    private final ConsumerFactory factory;
    private final String topic;
    private final TestStatistics statistics;

    TestRunner(ConsumerFactory factory) {
        this.topic = "Testing";
        this.factory = factory;
        this.producer = new TestingProducer(Duration.ofMillis(100), 2);
        this.statistics = new TestStatistics();
    }

    void startTest() throws Exception {
        runSingleConsumerTest();

        runTwoConsumersInSameGroupTest();

        runJoinLeaveGroupTest();
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    private void runSingleConsumerTest() throws Exception {
        logger.info("Run single consumer test.");

        final LcKafkaConsumer<Integer, String> client = factory.buildConsumer("cn/leancloud/kafka/consumer", statistics);
        client.subscribe(Collections.singletonList(topic));
        final int totalSent = producer.startFixedDurationTest(topic, Duration.ofSeconds(10)).get();

        try {
            waitTestDone(totalSent);
        } finally {
            client.close();
        }

        logger.info("Single consumer test finished. \n");
    }

    private void runTwoConsumersInSameGroupTest() throws Exception {
        logger.info("Run two consumers in same group test.");

        final LcKafkaConsumer<Integer, String> client = factory.buildConsumer("consumer-1", statistics);
        final LcKafkaConsumer<Integer, String> client2 = factory.buildConsumer("consumer-2", statistics);
        client.subscribe(Collections.singletonList(topic));
        client2.subscribe(Collections.singletonList(topic));
        final int totalSent = producer.startFixedDurationTest(topic, Duration.ofSeconds(10)).get();

        try {
            waitTestDone(totalSent);
        } finally {
            client.close();
            client2.close();
        }

        logger.info("Two consumers in same group test finished. \n");
    }

    private void runJoinLeaveGroupTest() throws Exception {
        logger.info("Run join leave group test.");

        final AtomicBoolean stopProducer = new AtomicBoolean();
        final CompletableFuture<Integer> producerSentCountFuture = producer.startNonStopTest(topic, stopProducer);
        final LcKafkaConsumer<Integer, String> lingerConsumer = factory.buildConsumer("linger-consumer", statistics);
        lingerConsumer.subscribe(Collections.singletonList(topic));

        List<LcKafkaConsumer<Integer, String>> postJoinConsumers = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            final LcKafkaConsumer<Integer, String> consumer = factory.buildConsumer("consumer-" + i, statistics);
            consumer.subscribe(Collections.singletonList(topic));
            postJoinConsumers.add(consumer);
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 5000));
        }

        for (LcKafkaConsumer<Integer, String> consumer : postJoinConsumers) {
            consumer.close();
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 5000));
        }

        stopProducer.set(true);
        int totalSent = producerSentCountFuture.get();

        try {
            waitTestDone(totalSent);
        } finally {
            lingerConsumer.close();
        }

        logger.info("Join leave group test finished. \n");
    }

    private void waitTestDone(int totalSent) {
        try {
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> statistics.getReceiveRecordsCount() >= totalSent);

            logger.info("Integration test finished. sent: {}, received: {}, duplicates: {}",
                    totalSent, statistics.getReceiveRecordsCount(), statistics.getDuplicateRecordsCount());
        } catch (Exception ex) {
            logger.error("Integration test got unexpected exception. sent: {}, received: {}, duplicates: {}",
                    totalSent, statistics.getReceiveRecordsCount(), statistics.getDuplicateRecordsCount(), ex);
        } finally {
            statistics.clear();
        }
    }
}
