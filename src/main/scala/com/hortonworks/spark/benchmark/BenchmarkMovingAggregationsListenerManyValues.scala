package com.hortonworks.spark.benchmark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.mutable

class BenchmarkMovingAggregationsListenerManyValues(args: Array[String])
  extends BaseBenchmarkMovingAggregationListener(args,
    "BenchmarkMovingAggregationsListenerToFsManyValues", "movingAggregationManyValues") {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    val columnExprsToFinalColumns: Seq[(String, Column)] =
      BenchmarkQueryHelper.createBunchOfColumnExpressions(ss, 20, "mod(value, 100)",
        "mod_ex_")

    val selectExprs = new mutable.ArrayBuffer[String](3 + columnExprsToFinalColumns.size)
    selectExprs ++= Seq("timestamp", "mod(value, 100) as mod", "value")
    selectExprs ++= columnExprsToFinalColumns.map(_._1)
    selectExprs += BenchmarkQueryHelper.createCaseExprStr(
      "mod(CAST(RANDN(0) * 1000 as INTEGER), 50)", 50, 10) + " as word"

    val groupByExprs = new mutable.ArrayBuffer[Column](3)
    groupByExprs ++= Seq(window($"timestamp", "1 minute", "10 seconds"), $"mod", $"word")

    val aggExprs = BenchmarkQueryHelper.createBunchOfAggExpressions(
      columnExprsToFinalColumns.map(_._2))

    df.withWatermark("timestamp", "10 seconds")
      .selectExpr(selectExprs: _*)
      .groupBy(groupByExprs: _*)
      .agg(aggExprs.take(1).last, aggExprs.drop(1): _*)
  }
}


object BenchmarkMovingAggregationsListenerManyValues {
  def main(args: Array[String]): Unit = {
    new BenchmarkMovingAggregationsListenerManyValues(args).runBenchmark()
  }

}
