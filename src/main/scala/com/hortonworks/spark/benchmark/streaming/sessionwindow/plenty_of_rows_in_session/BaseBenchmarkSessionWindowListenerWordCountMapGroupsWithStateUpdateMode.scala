package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_rows_in_session

import java.sql.Timestamp

import com.hortonworks.spark.benchmark.streaming.sessionwindow._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

case class Event(sessionId: String, timestamp: Timestamp)

case class SessionInfo(numEvents: Int,
                       startTimestampMs: Long,
                       endTimestampMs: Long) {

  /** Duration of the session, between the first and last events */
  def durationMs: Long = endTimestampMs - startTimestampMs
}

case class SessionUpdate(id: String,
                         startTimestampMs: Long,
                         endTimestampMs: Long,
                         durationMs: Long,
                         numEvents: Int,
                         expired: Boolean)

abstract class BaseBenchmarkSessionWindowListenerWordCountMapGroupsWithStateUpdateMode(
    args: Array[String],
    appName: String,
    queryName: String,
    outputMode: OutputMode)
  extends BaseBenchmarkSessionWindowListener(args, appName, queryName, outputMode) {

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

    // Sessionize the events. Track number of events, start and end timestamps of session, and
    // and report session updates.
    val sessionUpdates = events
      .groupByKey(event => event._1)
      .flatMapGroupsWithState[List[SessionInfo], SessionUpdate](
      outputMode, GroupStateTimeout.EventTimeTimeout()) {

      case (sessionId: String, events: Iterator[(String, Timestamp)],
      state: GroupState[List[SessionInfo]]) =>

        val sessionGapMillis = 10 * 1000

        def handleEvict(sessionId: String, state: GroupState[List[SessionInfo]]) = {
          state.getOption match {
            case Some(lst) =>
              // assuming sessions are sorted by session start timestamp
              val (evicted, kept) = lst.span {
                s => s.sessionEndTimestampMs < state.getCurrentWatermarkMs()
              }

              if (kept.isEmpty) {
                state.remove()
              } else {
                state.update(kept)
                state.setTimeoutTimestamp(kept.head.sessionEndTimestampMs)
              }

              outputMode match {
                case s if s == OutputMode.Append() =>
                  evicted.iterator.map(si => SessionUpdate(sessionId, si.sessionStartTimestampMs,
                    si.sessionEndTimestampMs, si.durationMs, si.numEvents))
                case s if s == OutputMode.Update() => Iterator.empty[SessionUpdate]
                case s => throw new UnsupportedOperationException(s"Not supported output mode $s")
              }

            case None =>
              state.remove()
              Iterator.empty[SessionUpdate]
          }
        }

        def handleEvents(sessionId: String, events: Iterator[(String, Timestamp)],
                         state: GroupState[List[SessionInfo]]) = {
          val prevSessionsIter = state.getOption match {
            case Some(lst) => lst.iterator
            case None => Iterator.empty[SessionInfo]
          }

          val newSessions = new mutable.MutableList[SessionInfo]()
          val updatedSessions = new mutable.MutableList[SessionInfo]()

          var currentSessionFromEvents: SessionInfo = _
          var currentSessionFromPrevSessions: SessionInfo = _
          var currentNewSession: SessionInfo = _
          var currentNewSessionIsUpdated = false

          // we're doing merge sort based on the fact both events and previous sessions are sorted.
          // we will apply merging session directly instead of dealing with list of sessions being merged.
          while (prevSessionsIter.hasNext || events.hasNext) {
            if (currentSessionFromEvents == null) {
              if (events.hasNext) {
                val event = events.next()
                currentSessionFromEvents = SessionInfo(event._2.getTime,
                  event._2.getTime + sessionGapMillis, 1)
              }
            }

            if (currentSessionFromPrevSessions == null) {
              if (prevSessionsIter.hasNext) {
                currentSessionFromPrevSessions = prevSessionsIter.next()
              }
            }

            var rowFromEvent = false
            val rowToApply = {
              if (currentSessionFromEvents == null) {
                currentSessionFromPrevSessions
              } else if (currentSessionFromPrevSessions == null) {
                rowFromEvent = true
                currentSessionFromEvents
              } else {
                if (currentSessionFromEvents.sessionStartTimestampMs <
                  currentSessionFromPrevSessions.sessionStartTimestampMs) {
                  rowFromEvent = true
                  currentSessionFromEvents
                } else {
                  currentSessionFromPrevSessions
                }
              }
            }

            // let it be empty to be fetched in next loop
            if (rowFromEvent) {
              currentSessionFromEvents = null
            } else {
              currentSessionFromPrevSessions = null
            }

            if (currentNewSession == null) {
              currentNewSession = rowToApply
              if (rowFromEvent) {
                currentNewSessionIsUpdated = true
              }
            } else {
              // check whether new session can be merged into one
              if (currentNewSession.sessionEndTimestampMs >= rowToApply.sessionStartTimestampMs) {
                // can be merged
                val newSessionEndTimestamp = if (currentNewSession.sessionEndTimestampMs > rowToApply.sessionEndTimestampMs) {
                  currentNewSession.sessionEndTimestampMs
                } else {
                  rowToApply.sessionEndTimestampMs
                }
                currentNewSession = currentNewSession.copy(sessionEndTimestampMs = newSessionEndTimestamp, numEvents = currentNewSession.numEvents + 1)

                if (rowFromEvent) {
                  currentNewSessionIsUpdated = true
                }
              } else {
                // new session appears, and current session can be closed
                newSessions += currentNewSession
                if (currentNewSessionIsUpdated) {
                  updatedSessions += currentNewSession
                }

                currentNewSession = rowToApply
                if (rowFromEvent) {
                  currentNewSessionIsUpdated = true
                }
              }
            }
          }

          // process the last session
          if (currentNewSession != null) {
            newSessions += currentNewSession
            if (currentNewSessionIsUpdated) {
              updatedSessions += currentNewSession
            }
          }

          state.update(newSessions.toList)

          // there must be at least one session available
          // set timeout to earliest sessions' session end: we will traverse and evict sessions
          state.setTimeoutTimestamp(newSessions.head.sessionEndTimestampMs)

          outputMode match {
            case s if s == OutputMode.Update() =>
              updatedSessions.iterator.map(si => SessionUpdate(sessionId, si.sessionStartTimestampMs,
                si.sessionEndTimestampMs, si.durationMs, si.numEvents))
            case s if s == OutputMode.Append() => Iterator.empty[SessionUpdate]
            case s => throw new UnsupportedOperationException(s"Not supported output mode $s")
          }
        }

        if (state.hasTimedOut) {
          handleEvict(sessionId, state)
        } else {
          handleEvents(sessionId, events, state)
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
