package cn.leancloud.kafka.consumer.integration;

public class TestContext {
    private final TestingProducer producer;
    private final ConsumerFactory factory;
    private final String topic;

    TestContext(String topic, TestingProducer producer, ConsumerFactory factory) {
        this.topic = topic;
        this.producer = producer;
        this.factory = factory;
    }

    public ConsumerFactory factory() {
        return factory;
    }

    public TestingProducer producer() {
        return producer;
    }

    public String topic() {
        return topic;
    }

    public TestStatistics newStatistics() {
        return new TestStatistics();
    }
}
