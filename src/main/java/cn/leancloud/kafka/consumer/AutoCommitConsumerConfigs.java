package cn.leancloud.kafka.consumer;

import javax.annotation.Nullable;

enum AutoCommitConsumerConfigs implements KafkaConfigsChecker {
    MAX_POLL_INTERVAL_MS("max.poll.interval.ms"),
    AUTO_COMMIT_INTERVAL_MS("auto.commit.interval.ms");

    private String config;
    @Nullable
    private String expectedValue;
    private boolean required;

    AutoCommitConsumerConfigs(String config) {
        this.config = config;
        this.expectedValue = null;
        this.required = true;
    }

    AutoCommitConsumerConfigs(String config, @Nullable String expectedValue, boolean required) {
        this.config = config;
        this.expectedValue = expectedValue;
        this.required = required;
    }

    @Override
    public String configName() {
        return config;
    }

    @Override
    public String expectedValue() {
        return expectedValue;
    }

    @Override
    public boolean required() {
        return required;
    }
}
