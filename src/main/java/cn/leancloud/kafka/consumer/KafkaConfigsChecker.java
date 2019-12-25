package cn.leancloud.kafka.consumer;

import java.util.Map;

interface KafkaConfigsChecker extends KafkaConfigs {
    default void check(Map<String, Object> configs) {
        Object val = configs.get(configName());

        if (val == null) {
            if (required()) {
                throw new IllegalArgumentException("expect \"" + configName() + "\" in kafka configs");
            }
        } else {
            if (expectedValue() != null && !val.equals(expectedValue())) {
                throw new IllegalArgumentException("kafka configs \"" + configName() + "\":" + val + ". (expect = " + expectedValue() + ")");
            }
        }
    }
}
