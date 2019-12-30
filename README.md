# Kafka Java Consumer

[![Build Status](https://api.travis-ci.org/leancloud/kafka-java-consumer.svg?branch=master)](https://travis-ci.org/leancloud/kafka-java-consumer)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Kafka is a greate lib which is very versatile and flexible, but many things may go wrong if you use it without great care and a good understanding about Kafka internals.  Next we will talk about some pitfalls on the consumer side which are easy to encounter with. 

Usually, after we have subscribed the consumer to some topics, we need a loop to do these things: 

* Fetch records from Kafka broker by using `poll` method on `KafkaConsumer`;
* Process the fetched records;
* Commit the offset of these fetched records, so they will not be consumed again;

We need to call `poll` constantly and ensure that the interval between each call should not too long, otherwise after a session timeout or a poll timeout, the broker may think our consumer is not alive and revoke every partitions assigned to the consumer. If we need to do a lot of things with the records we fetched, we may need to set the Kafka consumer configuration "max.poll.interval.ms" to a comparatively large value to give us enough time to process all these records. But it's not trival to set "max.poll.interval.ms" to a large value. The larger the "max.poll.interval.ms" value is, the longer time it's needed for a broker to realize that a consumer is dead when something wrong with the consuemr. In addition to tune the "max.poll.interval.ms" configuration, we can spare the polling thread only to poll records from broker and submit all the fetched records to a thread pool which is taking charge of processing these records. But to do it in this way, we need to pause the partitions of all the fetched records before processing them to prevent the polling threads from polling more records while the previous records are still processing. Of course, we should remember to resume a paused parition after we have processed all records from that partition. Futher more, after a partition reassignment, we should remember which partition we paused before the partition reassignemnt, and pause the paused partition again. 

Kafka provides a synchronous and a asynchronous way to commit offset of records. In addition to them, Kafka also provide way to commit for specific partition and offset, and way to commit all the records fetched at once.We should remember to commit all the processed records from a partition before this partition is revoked. We should remember to commit all the processed records before the consuemr shutdown. If we commit offset for a specific record, we should remember to plus one to the offset of that record, which means assuming the record to commit have partition 0 and offset 100, we should commit partition 0 to 101 instead of 100, otherwise that processed records will be fetched again. If a consumer were assigned a parition which have no records for a long time, we should still remember to commit the committed offset of that partition periodically, otherwise after the commit log of that partition was removed from broker, because of retention timeout, broker will not remember where the commit offset of that partition for the consumer was. If the consumer set Kafka consumer configuration "auto.offset.reset" to "earliest", the cosumer will poll the records it processed and committed after a reboot.  

All in all, Kafka is not a tool which can be used without thinking and doing some research. The purpose of this lib is to flatten these pits. With this lib, you can consume records from a subscribed topic and process them more safely and easily.

## Usage

Firstly, we need configurations for Kafka consumer. For example:

```Java
final Map<String, Object> configs = new HashMap<>();
configs.put("bootstrap.servers", "localhost:9092");
configs.put("group.id", "LeanCloud");
configs.put("auto.offset.reset", "earliest");
configs.put("max.poll.records", 10);
configs.put("max.poll.interval.ms", 30_000);
configs.put("key.deserializer", "...");
configs.put("value.deserializer", "...");
```

Then, define how you need to handle a record consumed from Kafka. Here we just log the consumed record:

```java
ConsumerRecordHandler<Integer, String> handler = record -> {
    logger.info("I got a record: {}", record);
};
```

Next, we need to choose the type of consumer to use. We have five kinds of consumers and each of them have different committing policy. Here is a simple specification for them:

commit policy | description
------ | ------------
automatic commit | Commit offsets of records fetched from kafka automatically in a fixed interval. 
sync commit | Commit offsets synchronously only when all the fetched records have been processed. 
async commit | Commit offsets asynchronously only when all the fetched records have been processed. If there are too many pending async commit requests or the last async commit request was failed, it'll switch to synchronous mode to commit synchronously and switch back when the next synchoronous commit success.
partial sync commit | Whenever there is a processed consumer record, only those records that have already been processed are committed synchronously, leaving the ones that have not been processed yet to be committed. 
partial async commit | Whenever there is a processed consumer record, only those records that have already been processed are committed asynchronously, leaving the ones that have not been processed yet to be committed. If there are too many pending async commit requests or the last async commit request was failed, it'll switch to synchronous mode to commit synchronously and switch back when the next synchoronous commit success.

Taking sync-committing consumer as an example, you can create a consumer with a thread pool and subscribe it to a topic like this:

```java
final LcKafkaConsumer<Integer, String> consumer = LcKafkaConsumerBuilder
                .newBuilder(configs, handler)
                // true means the LcKafkaConsumer should shutdown the input thread pool when it is shutting down
                .workerPool(Executors.newCachedThreadPool(), true)  
                .buildSync();
consumer.subscribe(Collections.singletonList("LeanCloud-Topic"));
```

Please note that we passed a `ExecutorService` to build the `LcKafkaConsumer` , all the records consumed from the subscribed topic will be handled by this `ExecutorService` using the input `ConsumerRecordHandler`. 

When we are done with this consumer, we need to close it:

```
consumer.close()
```

For all the APIs and descriptions of all the kinds of consumers, please refer to the Java Doc.

## License

Copyright 2019 LeanCloud. Released under the [MIT License](https://github.com/leancloud/filter-service/blob/master/LICENSE.md).

