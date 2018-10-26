package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_keys

import java.sql.Timestamp

import com.hortonworks.spark.benchmark.streaming.sessionwindow._
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.{DataFrame, SparkSession}

class BenchmarkSessionWindowListenerWordCountFlatMapGroupsWithState(
    conf: SessionWindowBenchmarkAppConf, appName: String, queryName: String)
  extends BaseBenchmarkSessionWindowListener(conf, appName, queryName) {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    // NOTE: in order to apply watermark we can't map typed columns to an object
    val events = df
      .selectExpr("CAST(CAST(value / 100 AS INTEGER) AS STRING) AS sessionId",
        "CAST(timestamp AS TIMESTAMP) AS eventTime")
      .as[(String, Timestamp)]
      .withWatermark("eventTime", "10 seconds")

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
