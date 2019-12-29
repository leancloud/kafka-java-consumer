package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.LcKafkaConsumer;

import java.time.Duration;
import java.util.Collections;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

class SingleConsumerTest implements IntegrationTest{
    @Override
    public String name() {
        return "Single-Consumer-Test";
    }

    @Override
    public void runTest(TestContext cxt, TestStatistics statistics) throws Exception {
        final LcKafkaConsumer<Integer, String> consumer = cxt.factory().buildConsumer(name(), statistics);
        consumer.subscribe(Collections.singletonList(cxt.topic()));
        final int totalSent = cxt.producer().startFixedDurationTest(cxt.topic(), Duration.ofSeconds(10)).get();

        try {
            statistics.recordTotalSent(totalSent);
            await().atMost(10, SECONDS)
                    .pollInterval(1, SECONDS)
                    .until(() -> statistics.getReceiveRecordsCount() == totalSent);
        } finally {
            consumer.close();
        }
    }
}
