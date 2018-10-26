package com.hortonworks.spark.benchmark.streaming.timewindow

import com.hortonworks.spark.benchmark.BenchmarkQueryHelper
import org.apache.spark.sql.functions.{avg, max, min, window}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BenchmarkMovingAggregationsListenerKeyMuchBigger(conf: TimeWindowBenchmarkAppConf)
  extends BaseBenchmarkMovingAggregationListener(
    conf,
    "BenchmarkMovingAggregationsListenerToFsKeyMuchBigger",
    "movingAggregationKeyMuchBigger") {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    df.withWatermark("timestamp", "10 seconds")
      .selectExpr(
        "timestamp", "mod(value, 100) as mod", "value",
        BenchmarkQueryHelper.createCaseExprStr(
          "mod(CAST(RANDN(0) * 1000 as INTEGER), 50)", 50, 1000) + " as word")
      .groupBy(
        window($"timestamp", "1 minute", "10 seconds"),
        $"mod", $"word")
      .agg(max("value").as("max_value"), min("value").as("min_value"), avg("value").as("avg_value"))
  }
}