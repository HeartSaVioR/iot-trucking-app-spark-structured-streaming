package com.hortonworks.spark.benchmark

import com.hortonworks.spark.utils.QueryListenerWriteProgressToFile
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseBenchmarkMovingAggregationListener(args: Array[String], appName: String,
                                                      queryName: String) {

  def applyOperations(ss: SparkSession, df: DataFrame): DataFrame

  def runBenchmark(): Unit = {
    val conf = new IotTruckingBenchmarkAppConf(args)
    val queryStatusFile = conf.queryStatusFile()
    val rateRowPerSecond = conf.rateRowPerSecond()
    val rateRampUpTimeSecond = conf.rateRampUpTimeSecond()

    val ss = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    ss.streams.addListener(new QueryListenerWriteProgressToFile(queryStatusFile))

    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", rateRowPerSecond)
      .option("rampUpTime", s"${rateRampUpTimeSecond}s")
      .load()

    df.printSchema()

    val query = applyOperations(ss, df)
      .writeStream
      .format("memory")
      .option("queryName", queryName)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }
}
