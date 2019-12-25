package cn.leancloud.kafka.consumer;

import javax.annotation.Nullable;
import java.util.Map;

interface KafkaConfigs {
    String configName();

    @Nullable
    String expectedValue();

    boolean required();

    default void set(Map<String, Object> configs, Object value) {
        configs.put(configName(), value);
    }

    default <T> T get(Map<String, Object> configs) {
        @SuppressWarnings("unchecked")
        T value = (T) configs.get(configName());
        return value;
    }
}
