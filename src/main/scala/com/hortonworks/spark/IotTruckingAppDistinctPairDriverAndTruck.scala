package com.hortonworks.spark

import com.hortonworks.spark.utils.{EventsDataSource, IotTruckingAppConf, QueryListenerSendingProgressToKafka}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.DataFrame

object IotTruckingAppDistinctPairDriverAndTruck {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()
    val failOnDataLoss = conf.failOnDataLoss()

    val ss = SparkSession
      .builder()
      .appName("IotTruckingAppDistinctPairDriverAndTruck")
      .getOrCreate()

    conf.queryStatusTopic.foreach(topic =>
      ss.streams.addListener(new QueryListenerSendingProgressToKafka(brokers, topic)))

    val speedEventDf: DataFrame = EventsDataSource.getTruckSpeedEventDf(ss, brokers,
      conf.speedEventsTopic(), failOnDataLoss)

    speedEventDf.printSchema()

    val deduplicatedDf = speedEventDf.dropDuplicates("driverId", "truckId")

    val query = EventsDataSource.runQueryWriteToKafka(ss, deduplicatedDf, brokers, conf.outputTopic(),
      conf.checkpoint(), Trigger.ProcessingTime("10 seconds"), OutputMode.Append())

    query.awaitTermination()
  }
}
