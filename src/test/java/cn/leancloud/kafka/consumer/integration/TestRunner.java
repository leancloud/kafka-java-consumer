package cn.leancloud.kafka.consumer.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;

public class TestRunner implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(TestRunner.class);

    private final TestingProducer producer;
    private final ConsumerFactory factory;
    private final TestContext context;

    TestRunner(String topic, ConsumerFactory factory) {
        this.factory = factory;
        this.producer = new TestingProducer(Duration.ofMillis(100), 2);
        this.context = new TestContext(topic, producer, factory);
    }

    void runTest(IntegrationTest test) throws Exception {
        logger.info("\n\n\n------------------------------------ Start {} with consumer: {} ------------------------------------\n",
                test.name(), factory.type());

        final TestStatistics statistics = context.newStatistics();
        try {
            test.runTest(context, statistics);

            logger.info("{} test finished. sent: {}, received: {}, duplicates: {}",
                    test.name(), statistics.getTotalSentCount(),
                    statistics.getReceiveRecordsCount(), statistics.getDuplicateRecordsCount());
        } catch (Exception ex) {
            logger.error("{} test got unexpected exception. sent: {}, received: {}, duplicates: {}", test.name(),
                    statistics.getTotalSentCount(), statistics.getReceiveRecordsCount(), statistics.getDuplicateRecordsCount(), ex);
            throw ex;
        } finally {
            statistics.clear();
        }

        logger.info("------------------------------------ {} with consumer: {} finished ------------------------------------\n",
                test.name(), factory.type());
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
