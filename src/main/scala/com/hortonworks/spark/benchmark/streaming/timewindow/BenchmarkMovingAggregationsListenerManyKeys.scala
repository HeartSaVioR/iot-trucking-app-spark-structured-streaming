package com.hortonworks.spark.benchmark.streaming.timewindow

import com.hortonworks.spark.benchmark.BenchmarkQueryHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class BenchmarkMovingAggregationsListenerManyKeys(args: Array[String])
  extends BaseBenchmarkMovingAggregationListener(args,
    "BenchmarkMovingAggregationsListenerToFsManyKeys", "movingAggregationFsManyKeys") {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

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

    df.withWatermark("timestamp", "10 seconds")
      .selectExpr(selectExprs: _*)
      .groupBy(groupByExprs: _*)
      .agg(aggExprs.take(1).last, aggExprs.drop(1): _*)
  }
}


object BenchmarkMovingAggregationsListenerManyKeys {
  def main(args: Array[String]): Unit = {
    new BenchmarkMovingAggregationsListenerManyKeys(args).runBenchmark()
  }

}
