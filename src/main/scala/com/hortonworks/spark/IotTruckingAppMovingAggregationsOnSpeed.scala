package com.hortonworks.spark

import com.hortonworks.spark.utils.{EventsDataSource, IotTruckingAppConf, QueryListenerSendingProgressToKafka}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object IotTruckingAppMovingAggregationsOnSpeed {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()
    val failOnDataLoss = conf.failOnDataLoss()

    val ss = SparkSession
      .builder()
      .appName("IotTruckingAppMovingAggregationsOnSpeed")
      .getOrCreate()

    conf.queryStatusTopic.foreach(topic =>
      ss.streams.addListener(new QueryListenerSendingProgressToKafka(brokers, topic)))

    import ss.implicits._

    val speedEventDf: DataFrame = EventsDataSource.getTruckSpeedEventDf(ss, brokers,
      conf.speedEventsTopic(), failOnDataLoss)

    speedEventDf.printSchema()

    import org.apache.spark.sql.functions._

    val aggregatedDf = speedEventDf
      .groupBy(
        window($"eventTimestamp", "1 hour", "10 minutes"),
        $"driverId")
      .agg(max("speed").as("max_speed"), min("speed").as("min_speed"), avg("speed").as("avg_speed"))

    val query = EventsDataSource.runQueryWriteToKafka(ss, aggregatedDf, brokers, conf.outputTopic(),
      conf.checkpoint(), Trigger.ProcessingTime("10 seconds"), OutputMode.Append())

    query.awaitTermination()
  }
}
