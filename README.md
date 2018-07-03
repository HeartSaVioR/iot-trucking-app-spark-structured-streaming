# IoT Trucking App with Spark Structured Streaming

This project reimplements HDF IoT Trucking Apps with Spark Structured Streaming.

* [trucking-iot](https://github.com/orendain/trucking-iot/tree/hadoop-summit-2017), Hadoop Summit 2017

This project provides couple of sample applications which leverage stream-stream join, window aggregation, deduplication respectively.

This project depends on HDP version of Spark, so you may want to modify Spark version before building the project to accomodate your installation of Spark.

Apps depend on Truck Geo Event as well as Truck Speed Event, which are well explained in [README.md in sam-trucking-data-utils](https://github.com/HeartSaVioR/sam-trucking-data-utils/blob/fix-for-spark-structured-streaming/README.md).

Apps also assume both truck geo events and truck speed events are ingested into Kafka topics, which can be easily done via [sam-trucking-data-utils](https://github.com/HeartSaVioR/sam-trucking-data-utils/blob/fix-for-spark-structured-streaming/README.md).

To run application on this project, please follow below steps:

> Build the project

`sbt assembly`

> Create Kafka topics for input events as well as result

```
$KAFKA_HOME/bin/kafka-topics.sh --create --topic truck_events_stream \
--zookeeper localhost:2181 --replication-factor 1 \
--partitions 10

$KAFKA_HOME/bin/kafka-topics.sh --create --topic truck_speed_events_stream \
--zookeeper localhost:2181 --replication-factor 1 \
--partitions 10

$KAFKA_HOME/bin/kafka-topics.sh --create --topic trucking_app_query_progress \
--zookeeper localhost:2181 --replication-factor 1 \
--partitions 10

$KAFKA_HOME/bin/kafka-topics.sh --create --topic trucking_app_result \
--zookeeper localhost:2181 --replication-factor 1 \
--partitions 10
```

> (optional) Create and set up checkpoint directory

You would like to set up directory for checkpointing. It could be either local directory if you plan to run the app in local environment, or HDFS-like remote directory if you plan to run the app in cluster.

> Generate Trucking events and ingest to kafka topics

Assuming we cloned `sam-trucking-data-utils` and checked out branch `fix-for-spark-structured-streaming`, and built the project:

```
nohup java -cp ./target/stream-simulator-jar-with-dependencies.jar \
hortonworks.hdf.sam.refapp.trucking.simulator.SimulationRunnerApp \
200000 \ hortonworks.hdf.sam.refapp.trucking.simulator.impl.domain.transport.Truck \
hortonworks.hdf.sam.refapp.trucking.simulator.impl.collectors.KafkaJsonEventCollector \
1 \
./routes/midwest/ \
100 \
localhost:9092 \
ALL_STREAMS \
NONSECURE &
```

> Run application (`IotTruckingAppJoinedAbnormalEvents` app for example)

```
$SPARK_HOME/bin/spark-submit \
--class com.hortonworks.spark.IotTruckingAppJoinedAbnormalEvents \
--master yarn \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 2g \
--executor-cores 1 \
./build/iot-trucking-app-spark-structured-streaming.jar \
--brokers localhost:9092 \
--checkpoint /tmp/apps/iot_trucking_app_joined_abnormal_events/checkpoints \
--geo-events-topic truck_events_stream \
--speed-events-topic truck_speed_events_stream \
--query-status-topic trucking_app_query_progress \
--output-topic trucking_app_result
```
