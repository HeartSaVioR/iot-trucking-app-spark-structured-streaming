package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_rows_in_session

import java.sql.Timestamp

import com.hortonworks.spark.benchmark.streaming.sessionwindow._
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.{DataFrame, SparkSession}

class BenchmarkSessionWindowListenerWordCountFlatMapGroupsWithState(
    conf: SessionWindowBenchmarkAppConf, appName: String, queryName: String)
  extends BaseBenchmarkSessionWindowListener(conf, appName, queryName) {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    // Split the lines into words, treat words as sessionId of events
    // NOTE: in order to apply watermark we can't map typed columns to an object
    val events = df
      .select($"value", $"timestamp")
      .as[(Long, Timestamp)]
      .flatMap { case (value, timestamp) =>
        TestSentences.SENTENCES.apply((value % 10).intValue()).split(" ")
          .map { word => (word, timestamp) }
      }
      .as[(String, Timestamp)]
      .withWatermark("_2", "10 seconds")

    val sessionGapMills = 10 * 1000 // 10 seconds

    val stateFunc = EventTimeSessionWindowFlatMapWithGroupsWithState.getStateFunc(
      conf.getSparkOutputMode, sessionGapMills)

    val sessionUpdates = events
      .groupByKey(event => event._1)
      .flatMapGroupsWithState[List[SessionInfo], SessionUpdate](
      conf.getSparkOutputMode, timeoutConf = GroupStateTimeout.EventTimeTimeout())(stateFunc)

    sessionUpdates.toDF()
  }
}