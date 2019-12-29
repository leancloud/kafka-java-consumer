package cn.leancloud.kafka.consumer.integration;

import cn.leancloud.kafka.consumer.LcKafkaConsumer;

public interface ConsumerFactory {
    String type();

    LcKafkaConsumer<Integer, String> buildConsumer(String consumerName, TestStatistics statistics);
}
