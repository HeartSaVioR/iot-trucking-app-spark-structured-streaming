package com.hortonworks.spark.benchmark

import com.hortonworks.spark.utils.{IotTruckingAppConf, QueryListenerWriteProgressToFile}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object BenchmarkMovingAggregationsListenerToFsValueMuchBigger {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()

    val ss = SparkSession
      .builder()
      .appName("BenchmarkMovingAggregationsListenerToFsValueMuchBigger")
      .getOrCreate()

    conf.queryStatusTopic.foreach(topic =>
      ss.streams.addListener(new QueryListenerWriteProgressToFile(brokers, topic)))

    import org.apache.spark.sql.functions._
    import ss.implicits._

    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", "10000")
      .load()

    df.printSchema()

    val outDf = df
      .withWatermark("timestamp", "10 seconds")
      .selectExpr(
      "timestamp", "mod(value, 100) as mod", "value",
        BenchmarkQueryHelper.createCaseExprStr(
          "mod(CAST(RANDN(0) * 1000 as INTEGER), 50)", 50, 1000) + " as word")
      .groupBy(
        window($"timestamp", "1 minute", "10 seconds"),
        $"mod")
      .agg(max("value").as("max_value"), min("value").as("min_value"), avg("value").as("avg_value"),
        last("word").as("word_last"))

    val query = outDf
      .writeStream
      .format("memory")
      .option("checkpointLocation", conf.checkpoint())
      .option("queryName", "movingAggregationValueMuchBigger")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

}
