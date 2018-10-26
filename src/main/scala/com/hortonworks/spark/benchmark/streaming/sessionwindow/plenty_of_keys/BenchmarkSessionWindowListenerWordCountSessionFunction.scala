package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_keys

import com.hortonworks.spark.benchmark.streaming.sessionwindow.{BaseBenchmarkSessionWindowListener, SessionWindowBenchmarkAppConf}
import org.apache.spark.sql.functions.{count, session_window}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BenchmarkSessionWindowListenerWordCountSessionFunction(
    conf: SessionWindowBenchmarkAppConf, appName: String, queryName: String)
  extends BaseBenchmarkSessionWindowListener(conf, appName, queryName) {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    val events = df.toDF()
      .selectExpr("CAST(CAST(value / 100 AS INTEGER) AS STRING) AS sessionId", "timestamp AS eventTime")
      .withWatermark("eventTime", "10 seconds")

    events.groupBy(
      session_window($"eventTime", "10 seconds") as 'session, 'sessionId)
      .agg(count("*").as("numEvents"))
      .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs", "numEvents")
  }

}