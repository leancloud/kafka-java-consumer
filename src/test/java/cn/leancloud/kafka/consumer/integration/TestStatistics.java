package cn.leancloud.kafka.consumer.integration;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class TestStatistics {
    private final Set<String> receivedRecords;
    private final AtomicInteger duplicateRecordsCounter;
    private final AtomicInteger totalSentCounter;

    TestStatistics() {
        this.receivedRecords = ConcurrentHashMap.newKeySet();
        this.duplicateRecordsCounter = new AtomicInteger();
        this.totalSentCounter = new AtomicInteger();
    }

    boolean recordReceivedRecord(ConsumerRecord<Integer, String> record) {
        if (!receivedRecords.add(record.value())) {
            duplicateRecordsCounter.incrementAndGet();
            return false;
        }
        return true;
    }

    int recordTotalSent(int sent) {
        return totalSentCounter.addAndGet(sent);
    }

    int getReceiveRecordsCount() {
        return receivedRecords.size();
    }

    int getDuplicateRecordsCount() {
        return duplicateRecordsCounter.get();
    }

    int getTotalSentCount() {
        return totalSentCounter.get();
    }

    void clear() {
        receivedRecords.clear();
        duplicateRecordsCounter.set(0);
        totalSentCounter.set(0);
    }
}
