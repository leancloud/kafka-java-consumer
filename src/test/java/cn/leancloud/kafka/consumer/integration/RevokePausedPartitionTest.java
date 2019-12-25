package cn.leancloud.kafka.consumer.integration;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class RevokePausedPartitionTest {
    private static final Logger logger = LoggerFactory.getLogger(RevokePausedPartitionTest.class);

    private static final String topic = "Testing";

    private Map<String, Object> configs;
    private Consumer<Integer, String> pausePartitionConsumer;
    private Consumer<Integer, String> normalConsumer;

    public RevokePausedPartitionTest() {
        configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        configs.put("auto.offset.reset", "earliest");
        configs.put("group.id", "2614911922612339122");
        configs.put("max.poll.records", 100);
        configs.put("max.poll.interval.ms", "5000");
        configs.put("key.deserializer", IntegerDeserializer.class.getName());
        configs.put("value.deserializer", StringDeserializer.class.getName());
    }

    void run() throws Exception {
        final Map<String, Object> producerConfigs = new HashMap<>();
        producerConfigs.put("bootstrap.servers", "localhost:9092");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs, new IntegerSerializer(), new StringSerializer());

        for (int i = 0; i < 100; i++) {
            final ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, null, "" + i);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Produce record failed", exception);
                }
            });
        }

        CompletableFuture<Void> rebalanceTrigger = new CompletableFuture<>();

        pausePartitionConsumer = new KafkaConsumer<>(configs);
        pausePartitionConsumer.subscribe(Collections.singletonList(topic), new RebalanceListener("paused consumer"));
        new Thread(new PausePartitionConsumerJob(
                pausePartitionConsumer,
                rebalanceTrigger))
                .start();

        rebalanceTrigger.whenComplete((r, t) -> {
            normalConsumer = new KafkaConsumer<>(configs);
            normalConsumer.subscribe(Collections.singletonList(topic), new RebalanceListener("normal consumer"));
            new Thread(new NormalConsumerJob(
                    normalConsumer))
                    .start();

            for (int i = 100; i < 200; i++) {
                final ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, null, "" + i);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Produce record failed", exception);
                    }
                });
            }
        });


    }

    private static class PausePartitionConsumerJob implements Runnable, Closeable {
        private Consumer<Integer, String> pausePartitionConsumer;
        private CompletableFuture<Void> rebalanceTrigger;
        private volatile boolean closed;

        public PausePartitionConsumerJob(Consumer<Integer, String> consumer,
                                         CompletableFuture<Void> rebalanceTrigger) {
            this.pausePartitionConsumer = consumer;
            this.rebalanceTrigger = rebalanceTrigger;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ConsumerRecords<Integer, String> records = pausePartitionConsumer.poll(100);
                    if (!records.isEmpty()) {
                        if (!rebalanceTrigger.isDone()) {
                            pausePartitionConsumer.pause(records.partitions());
                            handleRecords(records);
                            rebalanceTrigger.complete(null);
                        } else {
                            handleRecords(records);
                        }
                    }
                } catch (WakeupException ex) {
                    if (closed) {
                        break;
                    }
                } catch (Exception ex) {
                    logger.error("Paused consumer got unexpected exception", ex);
                }
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            pausePartitionConsumer.wakeup();
        }

        private void handleRecords(ConsumerRecords<Integer, String> records) {
            for (ConsumerRecord<Integer, String> record : records) {
                logger.info("receive msgs on pause partition consumer " + record);
            }
        }
    }

    private static class NormalConsumerJob implements Runnable, Closeable {
        private Consumer<Integer, String> consumer;
        private volatile boolean closed;

        public NormalConsumerJob(Consumer<Integer, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ConsumerRecords<Integer, String> records = consumer.poll(100);
                    if (!records.isEmpty()) {
                        handleRecords(records);
                    }
                } catch (WakeupException ex) {
                    if (closed) {
                        break;
                    }
                } catch (Exception ex) {
                    logger.error("Normal consumer got unexpected exception", ex);
                }
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            consumer.wakeup();
        }

        private void handleRecords(ConsumerRecords<Integer, String> records) {
            for (ConsumerRecord<Integer, String> record : records) {
                logger.info("receive msgs on normal consumer " + record);
            }
        }
    }


    private static class RebalanceListener implements ConsumerRebalanceListener {
        private String consumerName;

        public RebalanceListener(String consumerName) {
            this.consumerName = consumerName;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("Partitions " + partitions + " revoked from consumer: " + consumerName);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            logger.info("Partitions " + partitions + " assigned to consumer: " + consumerName);
        }
    }

    public static void main(String[] args) throws Exception {
        RevokePausedPartitionTest test = new RevokePausedPartitionTest();
        test.run();
    }

}
