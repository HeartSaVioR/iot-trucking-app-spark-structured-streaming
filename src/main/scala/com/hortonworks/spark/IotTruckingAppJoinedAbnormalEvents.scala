package com.hortonworks.spark

import com.hortonworks.spark.utils.{EventsDataSource, IotTruckingAppConf, QueryListenerSendingProgressToKafka}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.DataFrame

object IotTruckingAppJoinedAbnormalEvents {
  // this is simplified version of Streaming-Analytics-Trucking-Ref-App to make it compatible with
  // Spark Structured Streaming

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.functions.expr

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()

    val ss = SparkSession
      .builder()
      .appName("IotTruckingAppJoinedAbnormalEvents")
      .getOrCreate()

    conf.queryStatusTopic.foreach(topic =>
      ss.streams.addListener(new QueryListenerSendingProgressToKafka(brokers, topic)))

    import ss.implicits._

    val geoEventDf: DataFrame = EventsDataSource.getTruckGeoEventDf(ss, brokers,
      conf.geoEventsTopic())
    val speedEventDf: DataFrame = EventsDataSource.getTruckSpeedEventDf(ss, brokers,
      conf.speedEventsTopic())

    geoEventDf.printSchema()
    speedEventDf.printSchema()

    val joinedDf = geoEventDf.as("geo").join(speedEventDf.as("speed"),
      expr("""
        geo.driverId = speed.driverId
        AND geo.truckId = speed.truckId
        AND geo.eventTimestamp >= speed.eventTimestamp
        AND geo.eventTimestamp <= speed.eventTimestamp + interval 1 second
      """), "inner")

    val abnormalEventsDf = joinedDf
      .filter("eventType <> 'Normal'")
      .select($"geo.driverId".as("driverId"), $"geo.driverName".as("driverName"),
        $"speed.route".as("route"), $"speed.speed".as("speed"))

    val query = EventsDataSource.runQueryWriteToKafka(ss, abnormalEventsDf, brokers, conf.outputTopic(),
      conf.checkpoint(), Trigger.ProcessingTime("10 seconds"), OutputMode.Append())

    query.awaitTermination()
  }
}
