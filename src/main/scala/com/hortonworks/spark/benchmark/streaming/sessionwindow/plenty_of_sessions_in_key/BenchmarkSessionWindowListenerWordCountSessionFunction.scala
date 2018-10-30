package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_sessions_in_key

import com.hortonworks.spark.benchmark.BenchmarkQueryHelper
import com.hortonworks.spark.benchmark.streaming.sessionwindow.{BaseBenchmarkSessionWindowListener, SessionWindowBenchmarkAppConf, TestSentences}
import org.apache.spark.sql.functions.{count, session_window}
import org.apache.spark.sql.{DataFrame, SparkSession}

class BenchmarkSessionWindowListenerWordCountSessionFunction(
    conf: SessionWindowBenchmarkAppConf, appName: String, queryName: String)
  extends BaseBenchmarkSessionWindowListener(conf, appName, queryName) {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    // Split the lines into words, treat words as sessionId of events
    // 60 * 60 * 24 * 7 = 7 days
    // 60 * 1 = 1 min
    // so it adds half of events as late events, and these events will be added
    // within 7 days via 1 min gap
    // we divide timestamp by 60 and multiply timestamp by 60 to get rid of remainder of 1 mins
    // session gap is defined as 30 seconds hence sessions are unlikely be merged into big one
    // NOTE: in order to apply watermark we can't map typed columns to an object
    val wordValueExpr = "mod(floor(value / 10), 100) as wordValue"
    val calculateLateEventTimestamp = "floor(CAST(timestamp AS LONG) / 60) * 60 - (60 * mod(value, 60 * 24 * 7))"

    val events = df.toDF()
      .selectExpr(wordValueExpr,
        "CASE WHEN mod(value, 2) == 0 THEN timestamp " +
        s"ELSE CAST($calculateLateEventTimestamp AS TIMESTAMP) END " +
        "AS eventTime")
      .selectExpr(BenchmarkQueryHelper.createCaseExprStr("wordValue", 100, 10) + " as word",
        "eventTime")
      .withWatermark("eventTime", "7 days")

    events.groupBy(
      session_window($"eventTime", "30 seconds") as 'session, 'word)
      .agg(count("*").as("numEvents"))
      .selectExpr("word", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
        "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs", "numEvents")
  }

}
