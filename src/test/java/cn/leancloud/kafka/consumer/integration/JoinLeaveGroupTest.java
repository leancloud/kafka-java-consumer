package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.LcKafkaConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class JoinLeaveGroupTest implements IntegrationTest{
    @Override
    public String name() {
        return "Join-Leave-Group";
    }

    @Override
    public void runTest(TestContext cxt, TestStatistics statistics) throws Exception {
        final AtomicBoolean stopProducer = new AtomicBoolean();
        final CompletableFuture<Integer> producerSentCountFuture = cxt.producer().startNonStopTest(cxt.topic(), stopProducer);
        final LcKafkaConsumer<Integer, String> lingerConsumer = cxt.factory().buildConsumer("linger-consumer", statistics);
        lingerConsumer.subscribe(Collections.singletonList(cxt.topic()));

        final List<LcKafkaConsumer<Integer, String>> postJoinConsumers = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            final LcKafkaConsumer<Integer, String> consumer = cxt.factory().buildConsumer("consumer-" + i, statistics);
            consumer.subscribe(Collections.singletonList(cxt.topic()));
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
            statistics.recordTotalSent(totalSent);
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> statistics.getReceiveRecordsCount() == totalSent);
        } finally {
            lingerConsumer.close();
        }
    }
}
