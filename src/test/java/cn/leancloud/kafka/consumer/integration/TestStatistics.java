package cn.leancloud.kafka.consumer.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class TestStatistics {
    private final Set<String> receivedRecords;
    private final AtomicInteger duplicateRecordsCounter;

    TestStatistics() {
        this.receivedRecords = ConcurrentHashMap.newKeySet();
        this.duplicateRecordsCounter = new AtomicInteger();
    }

    boolean recordReceivedRecord(ConsumerRecord<Integer, String> record) {
        if (!receivedRecords.add(record.value())) {
            duplicateRecordsCounter.incrementAndGet();
            return false;
        }
        return true;
    }

    int getReceiveRecordsCount() {
        return receivedRecords.size();
    }

    int getDuplicateRecordsCount() {
        return duplicateRecordsCounter.get();
    }

    void clear() {
        receivedRecords.clear();
        duplicateRecordsCounter.set(0);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final TestStatistics that = (TestStatistics) o;
        return Objects.equals(receivedRecords, that.receivedRecords) &&
                Objects.equals(duplicateRecordsCounter, that.duplicateRecordsCounter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(receivedRecords, duplicateRecordsCounter);
    }

    @Override
    public String toString() {
        return "TestStatistics{" +
                "receivedRecords=" + receivedRecords +
                ", duplicateRecordsCounter=" + duplicateRecordsCounter +
                '}';
    }
}
