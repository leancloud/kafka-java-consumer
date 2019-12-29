package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.LcKafkaConsumer;

import java.time.Duration;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class TwoConsumerTest implements IntegrationTest{
    @Override
    public String name() {
        return "Two-Consumer-Test";
    }

    @Override
    public void runTest(TestContext context, TestStatistics statistics) throws Exception {
        final LcKafkaConsumer<Integer, String> consumer = context.factory().buildConsumer("consumer-1", statistics);
        final LcKafkaConsumer<Integer, String> consumer2 = context.factory().buildConsumer("consumer-2", statistics);
        consumer.subscribe(Collections.singletonList(context.topic()));
        consumer2.subscribe(Collections.singletonList(context.topic()));
        final int totalSent = context.producer().startFixedDurationTest(context.topic(), Duration.ofSeconds(10)).get();

        try {
            statistics.recordTotalSent(totalSent);
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> statistics.getReceiveRecordsCount() == totalSent);
        } finally {
            consumer.close();
            consumer2.close();
        }
    }
}
