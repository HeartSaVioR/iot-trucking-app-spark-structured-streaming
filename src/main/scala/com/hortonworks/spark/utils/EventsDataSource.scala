package com.hortonworks.spark.utils

import org.apache.spark.sql.functions.{from_json, struct, to_json}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object EventsDataSource {

  def getTruckSpeedEventDf(ss: SparkSession, bootstrapServers: String, topic: String,
                           failOnDataLoss: Boolean): Dataset[Row] = {
    import ss.implicits._

    val speedEventDf = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", failOnDataLoss)
      .load()

    val speedSchema = StructType(Seq(
      StructField("eventTime", StringType, nullable = false),
      StructField("eventSource", StringType, nullable = false),
      StructField("truckId", IntegerType, nullable = false),
      StructField("driverId", IntegerType, nullable = false),
      StructField("driverName", StringType, nullable = false),
      StructField("routeId", IntegerType, nullable = false),
      StructField("route", StringType, nullable = false),
      StructField("speed", IntegerType, nullable = false)
    ))

    speedEventDf
      .selectExpr("CAST(value AS STRING) as value")
      .as[String]
      .select(from_json($"value", schema = speedSchema).as("data"))
      .selectExpr("data.*", "to_timestamp(data.eventTime, 'yyyy-MM-dd HH:mm:ss.SSS') AS eventTimestamp")
      .withWatermark("eventTimestamp", "1 hour")
  }

  def getTruckGeoEventDf(ss: SparkSession, bootstrapServers: String, topic: String,
                         failOnDataLoss: Boolean): Dataset[Row] = {
    import ss.implicits._

    val geoEventDf = ss
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", failOnDataLoss)
      .load()

    val geoSchema = StructType(Seq(
      StructField("eventTime", StringType, nullable = false),
      StructField("eventSource", StringType, nullable = false),
      StructField("truckId", IntegerType, nullable = false),
      StructField("driverId", IntegerType, nullable = false),
      StructField("driverName", StringType, nullable = false),
      StructField("routeId", IntegerType, nullable = false),
      StructField("route", StringType, nullable = false),
      StructField("eventType", StringType, nullable = false),
      StructField("latitude", DoubleType, nullable = false),
      StructField("longitude", DoubleType, nullable = false),
      StructField("correlationId", DoubleType, nullable = false)
    ))

    geoEventDf
      .selectExpr("CAST(value AS STRING) as value")
      .as[String]
      .select(from_json($"value", schema = geoSchema).as("data"))
      // MM/dd/yyyy HH:mm:ss
      .selectExpr("data.*", "to_timestamp(data.eventTime, 'yyyy-MM-dd HH:mm:ss.SSS') AS eventTimestamp")
      .withWatermark("eventTimestamp", "1 hour")
  }

  def runQueryWriteToKafka(ss: SparkSession, df: DataFrame, brokerServers: String,
                           outTopic: String, checkpointLocation: String, trigger: Trigger,
                           outputMode: OutputMode): StreamingQuery = {
    import ss.implicits._

    df.select(to_json(struct($"*")).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokerServers)
      .option("topic", outTopic)
      .option("checkpointLocation", checkpointLocation)
      .trigger(trigger)
      .outputMode(outputMode)
      .start()
  }
}
