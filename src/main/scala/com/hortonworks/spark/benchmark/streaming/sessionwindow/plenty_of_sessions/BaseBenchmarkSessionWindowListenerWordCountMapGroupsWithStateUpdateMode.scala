package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_sessions

import java.sql.Timestamp

import com.hortonworks.spark.benchmark.streaming.sessionwindow._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Event(sessionId: String, timestamp: Timestamp)

case class SessionInfo(numEvents: Int,
                       startTimestampMs: Long,
                       endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

case class SessionUpdate(id: Long,
                         startTimestampMs: Long,
                         endTimestampMs: Long,
                         durationMs: Long,
                         numEvents: Int,
                         expired: Boolean)

class BaseBenchmarkSessionWindowListenerWordCountMapGroupsWithStateUpdateMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListener(
    args,
    "SessionWindowWordCountAsMapGroupsWithStateUpdateMode_PlentyOfSessions",
    "SessionWindowWordCountAsMapGroupsWithStateUpdateMode_PlentyOfSessions",
    OutputMode.Update()) {

  override def applyOperations(ss: SparkSession, df: DataFrame): DataFrame = {
    import ss.implicits._

    // NOTE: in order to apply watermark we can't map typed columns to an object
    val events = df
      .selectExpr("CAST(value / 100 AS INTEGER) AS sessionId", "CAST(timestamp AS TIMESTAMP) AS eventTime")
      .as[(Long, Timestamp)]
      .withWatermark("eventTime", "10 seconds")

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event._1)
      .mapGroupsWithState[SessionInfo, SessionUpdate](GroupStateTimeout.EventTimeTimeout()) {

      case (sessionId: Long, events: Iterator[(Long, Timestamp)], state: GroupState[SessionInfo]) =>

        // If timed out, then remove session and send final update
        if (state.hasTimedOut) {
          val finalUpdate =
            SessionUpdate(sessionId, state.get.startTimestampMs, state.get.endTimestampMs,
              state.get.durationMs, state.get.numEvents, expired = true)
          state.remove()
          finalUpdate
        } else {
          // Update start and end timestamps in session
          // actually this is not a correct implementation, but we ignore for performance comparison
          val timestamps = events.map(_._2.getTime).toSeq
          val updatedSession = if (state.exists) {
            val oldSession = state.get
            SessionInfo(
              oldSession.numEvents + timestamps.size,
              oldSession.startTimestampMs,
              math.max(oldSession.endTimestampMs, timestamps.max))
          } else {
            SessionInfo(timestamps.size, timestamps.min, timestamps.max)
          }
          state.update(updatedSession)

          // Set timeout such that the session will be expired if no data received for 10 seconds
          state.setTimeoutTimestamp(timestamps.max, "10 seconds")
          SessionUpdate(sessionId, state.get.startTimestampMs, state.get.endTimestampMs,
            state.get.durationMs, state.get.numEvents, expired = false)
        }
    }

    sessionUpdates.toDF()
  }
}

object BaseBenchmarkSessionWindowListenerWordCountMapGroupsWithStateUpdateMode {
  def main(args: Array[String]): Unit = {
    new BaseBenchmarkSessionWindowListenerWordCountMapGroupsWithStateUpdateMode(args).runBenchmark()
  }

}
