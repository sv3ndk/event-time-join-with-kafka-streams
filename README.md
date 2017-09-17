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

The mood event generator should produce beautiful consultant's recommendations, as follows: 

```
 {"ingestion_time": 689, "consultant": "Adrian", "event_time": 131, "recommendation": "implement intuitive vortals"}
 {"ingestion_time": 697, "consultant": "Sheila", "event_time": 520, "recommendation": "seize proactive interfaces"}
 {"ingestion_time": 782, "consultant": "Tammy", "event_time": 629, "recommendation": "orchestrate efficient platforms"}
 {"ingestion_time": 949, "consultant": "Rhonda", "event_time": 257, "recommendation": "matrix integrated web services"}
 {"ingestion_time": 995, "consultant": "Rhonda", "event_time": 59, "recommendation": "enable B2C web-readiness"}
 {"ingestion_time": 1000, "consultant": "Adrian", "event_time": 603, "recommendation": "morph wireless initiatives"}
 {"ingestion_time": 1009, "consultant": "Scott", "event_time": 957, "recommendation": "orchestrate compelling platforms"}
 {"ingestion_time": 1029, "consultant": "Tammy", "event_time": 693, "recommendation": "generate vertical portals"}
 {"ingestion_time": 1078, "consultant": "Scott", "event_time": 526, "recommendation": "harness virtual schemas"}
 {"ingestion_time": 1127, "consultant": "Adrian", "event_time": 567, "recommendation": "grow integrated vortals"}
 {"ingestion_time": 1189, "consultant": "Tammy", "event_time": 1057, "recommendation": "revolutionize visionary content"}
 {"ingestion_time": 1284, "consultant": "Tammy", "event_time": 1255, "recommendation": "drive wireless e-tailers"}
 {"ingestion_time": 1297, "consultant": "Scott", "event_time": 973, "recommendation": "embrace mission-critical content"}
 {"ingestion_time": 1310, "consultant": "Tammy", "event_time": 1306, "recommendation": "embrace back-end networks"}
 {"ingestion_time": 1323, "consultant": "Tammy", "event_time": 1034, "recommendation": "target transparent systems"}
 {"ingestion_time": 1349, "consultant": "Tammy", "event_time": 1305, "recommendation": "expedite virtual info-mediaries"}
 {"ingestion_time": 1386, "consultant": "Scott", "event_time": 738, "recommendation": "engineer strategic e-business"}
 {"ingestion_time": 1399, "consultant": "Tammy", "event_time": 1106, "recommendation": "morph strategic networks"}
 {"ingestion_time": 1514, "consultant": "Adrian", "event_time": 1406, "recommendation": "innovate impactful content"}
 ```
 
 Which are generated with python Faker's [`bs()` method ](https://faker.readthedocs.io/en/latest/providers/faker.providers.company.html) (which stands for "business statement", right?)


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