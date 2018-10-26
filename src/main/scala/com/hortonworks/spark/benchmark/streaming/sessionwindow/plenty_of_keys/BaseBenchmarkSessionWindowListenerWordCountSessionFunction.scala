package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_keys

import com.hortonworks.spark.benchmark.streaming.sessionwindow.BaseBenchmarkSessionWindowListener
import org.apache.spark.sql.functions.{count, session_window}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseBenchmarkSessionWindowListenerWordCountSessionFunction(args: Array[String],
                                                                          appName: String,
                                                                          queryName: String,
                                                                          outputMode: OutputMode)
  extends BaseBenchmarkSessionWindowListener(args, appName, queryName, outputMode) {

      override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
        import ss.implicits._

        val events = df.toDF()
          .selectExpr("CAST(value / 100 AS INTEGER) AS sessionId", "CAST(timestamp AS TIMESTAMP) AS eventTime")
          .withWatermark("eventTime", "10 seconds")

        events.groupBy(
          session_window($"eventTime", "10 seconds") as 'session, 'sessionId)
          .agg(count("*").as("numEvents"))
          .selectExpr("sessionId", "CAST(session.start AS LONG)", "CAST(session.end AS LONG)",
            "CAST(session.end AS LONG) - CAST(session.start AS LONG) AS durationMs", "numEvents")
      }

  }
