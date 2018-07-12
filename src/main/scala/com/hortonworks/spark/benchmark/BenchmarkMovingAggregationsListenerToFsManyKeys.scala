package com.hortonworks.spark.benchmark

import com.hortonworks.spark.utils.{IotTruckingAppConf, QueryListenerWriteProgressToFile}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

import scala.collection.mutable

object BenchmarkMovingAggregationsListenerToFsManyKeys {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()

    val ss = SparkSession
      .builder()
      .appName("BenchmarkMovingAggregationsListenerToFsManyKeys")
      .getOrCreate()

    conf.queryStatusTopic.foreach(topic =>
      ss.streams.addListener(new QueryListenerWriteProgressToFile(brokers, topic)))

    import ss.implicits._

    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", "10000")
      .load()

    df.printSchema()

    val columnExprsToFinalColumns: Seq[(String, Column)] =
      BenchmarkQueryHelper.createBunchOfColumnExpressions(ss, 20, "mod(value, 100)", "mod_ex_")

    val selectExprs = mutable.ArrayBuffer[String]("timestamp", "mod(value, 100) as mod", "value")
    selectExprs ++= columnExprsToFinalColumns.map(_._1)
    selectExprs += BenchmarkQueryHelper.createCaseExprStr(
      "mod(CAST(RANDN(0) * 1000 as INTEGER), 50)", 50, 10) + " as word"

    val groupByExprs = new mutable.ArrayBuffer[Column](3 + columnExprsToFinalColumns.size)
    groupByExprs ++= Seq(window($"timestamp", "1 minute", "10 seconds"), $"mod", $"word")
    groupByExprs ++= columnExprsToFinalColumns.map(_._2)

    val aggExprs = Seq(max("value").as("max_value"), min("value").as("min_value"),
      avg("value").as("avg_value"))

    val outDf = df
      .withWatermark("timestamp", "10 seconds")
      .selectExpr(selectExprs: _*)
      .groupBy(groupByExprs: _*)
      .agg(aggExprs.take(1).last, aggExprs.drop(1): _*)

    val query = outDf
      .writeStream
      .format("memory")
      .option("checkpointLocation", conf.checkpoint())
      .option("queryName", "movingAggregationFsManyKeys")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

}
