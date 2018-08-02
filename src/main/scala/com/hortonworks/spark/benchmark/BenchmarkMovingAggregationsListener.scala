package com.hortonworks.spark.benchmark

import org.apache.spark.sql.functions.{avg, max, min, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BenchmarkMovingAggregationsListener(args: Array[String])
  extends BaseBenchmarkMovingAggregationListener(args,
    "BenchmarkMovingAggregationsListenerToFs", "movingAggregation") {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    df.withWatermark("timestamp", "10 seconds")
      .selectExpr(
        "timestamp", "mod(value, 100) as mod", "value",
        BenchmarkQueryHelper.createCaseExprStr(
          "mod(CAST(RANDN(0) * 1000 as INTEGER), 50)", 50, 10) + " as word")
      .groupBy(
        window($"timestamp", "1 minute", "10 seconds"),
        $"mod", $"word")
      .agg(max("value").as("max_value"), min("value").as("min_value"), avg("value").as("avg_value"))
  }
}

object BenchmarkMovingAggregationsListener {
  def main(args: Array[String]): Unit = {
    new BenchmarkMovingAggregationsListener(args).runBenchmark()
  }

}
