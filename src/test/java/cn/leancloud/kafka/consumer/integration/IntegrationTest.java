package cn.leancloud.kafka.consumer.integration;

public interface IntegrationTest {
    String name();

    void runTest(TestContext context, TestStatistics statistics) throws Exception;
}
