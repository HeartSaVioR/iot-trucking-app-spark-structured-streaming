package com.hortonworks.spark.benchmark.streaming.timewindow

import java.nio.file.Files

import com.hortonworks.spark.utils.QueryListenerWriteProgressToFile

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseBenchmarkMovingAggregationListener(conf: TimeWindowBenchmarkAppConf,
                                                      appName: String, queryName: String) {

  def applyOperations(ss: SparkSession, df: DataFrame): DataFrame

  def runBenchmark(): Unit = {
    val rateRowPerSecond = conf.rateRowPerSecond()
    val rateRampUpTimeSecond = conf.rateRampUpTimeSecond()

    val ss = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    conf.queryStatusFile.foreach { file =>
      ss.streams.addListener(new QueryListenerWriteProgressToFile(file))
    }

    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", rateRowPerSecond)
      .option("rampUpTime", s"${rateRampUpTimeSecond}s")
      .load()

    df.printSchema()

    var streamWriter = applyOperations(ss, df)
      .writeStream
      .format("console")
      .option("queryName", queryName)
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode(conf.getSparkOutputMode)

    conf.checkpointDirectory.foreach { chkDir =>
      streamWriter = streamWriter.option("checkpointLocation", chkDir)
    }

    val query = streamWriter.start()

    query.awaitTermination()
  }
}
