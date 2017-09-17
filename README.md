# Event-time join with kafka Streams

This is a proof of concept scala project to illustrate event-time join in Kafka Stream 0.11.0.0. 

The full description is available in my blog post: [Event-time join with kafka Streams]()

## Generate test data

In order to play with this POC, you first need to create the test topics: 

```sh
> kafka-topics --create --zookeeper localhost:2181 --topic etj-moods --replication-factor 1 --partitions 4
> kafka-topics --create --zookeeper localhost:2181 --topic etj-events --replication-factor 1 --partitions 4
```


You can the execute then start the python scripts in `gen_data`: 

```sh
> python gen_moods.py
> python gen_events.py
```

I recommend starting them together so produced timestamps are similar and kafka stream is able to apply flow control (TODO: review this: this might no longer be necessary since I handle timestamps explicitly in the scripts). 


You can check that have been posted to kafka as follows: 

```sh
>kafka-console-consumer --bootstrap-server localhost:9092 --topic etj-moods --from-beginning
>kafka-console-consumer --bootstrap-server localhost:9092 --topic etj-events --from-beginning
```

Since Kafka is persistent we're using it more as a storage than as a communication channel here, so this only needs to be executed once. THe stream processing will automatically start processing them from the start.

## Run the Kafka Stream application

Just launch the main [EventTimeJoinExampleApp.scala](src/main/scala/svend/example/eventimejoin/EventTimeJoinExampleApp.scala). 

Note that since we set a different application ID at each startup, this is equivalent to reseting the offset of the application to 0 at each startup, which is rather dirty but handy for tests. Do not do that with a real application ^^

```scala
p.put(StreamsConfig.APPLICATION_ID_CONFIG, s"event-time-join-example-${System.currentTimeMillis()}")
```