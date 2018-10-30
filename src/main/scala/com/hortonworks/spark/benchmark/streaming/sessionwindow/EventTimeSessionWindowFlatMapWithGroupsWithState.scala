package com.hortonworks.spark.benchmark.streaming.sessionwindow

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{GroupState, OutputMode}

case class SessionInfo(sessionStartTimestampMs: Long,
                       sessionEndTimestampMs: Long,
                       numEvents: Int) {

  /** Duration of the session, between the first and last events + session gap */
  def durationMs: Long = sessionEndTimestampMs - sessionStartTimestampMs
}

case class SessionUpdate(id: String,
                         sessionStartTimestampSecs: Long,
                         sessionEndTimestampSecs: Long,
                         durationSecs: Long,
                         numEvents: Int)

object EventTimeSessionWindowFlatMapWithGroupsWithState {
  def getStateFunc(outputMode: OutputMode, sessionGapMills: Long)
    : (String, Iterator[(String, Timestamp)], GroupState[List[SessionInfo]]) => Iterator[SessionUpdate] = {
    val stateFunc: (String, Iterator[(String, Timestamp)], GroupState[List[SessionInfo]])
      => Iterator[SessionUpdate] =
      (sessionId: String, events: Iterator[(String, Timestamp)],
       state: GroupState[List[SessionInfo]]) => {

        def handleEvict(sessionId: String, state: GroupState[List[SessionInfo]])
        : Iterator[SessionUpdate] = {
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
                  evicted.iterator.map(si => SessionUpdate(sessionId,
                    si.sessionStartTimestampMs / 1000,
                    si.sessionEndTimestampMs / 1000,
                    si.durationMs / 1000, si.numEvents))
                case s if s == OutputMode.Update() => Seq.empty[SessionUpdate].iterator
                case s => throw new UnsupportedOperationException(s"Not supported output mode $s")
              }

            case None =>
              state.remove()
              Seq.empty[SessionUpdate].iterator
          }
        }

        def mergeSession(session1: SessionInfo, session2: SessionInfo): SessionInfo = {
          SessionInfo(
            sessionStartTimestampMs = Math.min(session1.sessionStartTimestampMs,
              session2.sessionStartTimestampMs),
            sessionEndTimestampMs = Math.max(session1.sessionEndTimestampMs,
              session2.sessionEndTimestampMs),
            numEvents = session1.numEvents + session2.numEvents)
        }

        def handleEvents(sessionId: String, events: Iterator[(String, Timestamp)],
                         state: GroupState[List[SessionInfo]]): Iterator[SessionUpdate] = {

          import java.{util => ju}
          import scala.collection.mutable
          import collection.JavaConverters._

          // we assume only previous sessions are sorted: events are not guaranteed to be sorted.
          // we also assume the number of sessions for each key is not huge, which is valid
          // unless end users set huge watermark delay as well as smaller session gap.

          val newSessions: ju.LinkedList[SessionInfo] = state.getOption match {
            case Some(lst) => new ju.LinkedList[SessionInfo](lst.asJava)
            case None => new ju.LinkedList[SessionInfo]()
          }

          // this is to track the change of sessions for update mode
          // if you define "update" as returning whole new sessions on given key,
          // you can remove this and logic to track sessions
          val updatedSessions = new mutable.ListBuffer[SessionInfo]()

          while (events.hasNext) {
            val ev = events.next()

            // convert each event to one of session window
            val event = SessionInfo(ev._2.getTime, ev._2.getTime + sessionGapMills, 1)

            // find matched session
            var index = 0
            var updated = false
            while (!updated && index < newSessions.size()) {
              val session = newSessions.get(index)
              if (event.sessionEndTimestampMs < session.sessionStartTimestampMs) {
                // no matched session, and following sessions will not be matched
                newSessions.add(index, event)
                updated = true
                updatedSessions += event
              } else if (event.sessionStartTimestampMs > session.sessionEndTimestampMs) {
                // continue to next session
                index += 1
              } else {
                // matched: update session
                var newSession = session.copy(
                  sessionStartTimestampMs = Math.min(session.sessionStartTimestampMs,
                    event.sessionStartTimestampMs),
                  sessionEndTimestampMs = Math.max(session.sessionEndTimestampMs,
                    event.sessionEndTimestampMs),
                  numEvents = session.numEvents + event.numEvents)

                // we are going to replace previous session with new session, so previous session
                // should be removed from updated sessions
                // same occurs below if statements
                updatedSessions -= session

                // check for a chance to concatenate new session and next session
                if (index + 1 < newSessions.size()) {
                  val nextSession = newSessions.get(index + 1)
                  if (newSession.sessionEndTimestampMs <= nextSession.sessionStartTimestampMs) {
                    newSession = mergeSession(newSession, nextSession)

                    updatedSessions -= nextSession
                    newSessions.remove(index + 1)
                  }
                }

                // check for a chance to concatenate new session and previous session
                if (index - 1 >= 0) {
                  val prevSession = newSessions.get(index - 1)
                  if (newSession.sessionEndTimestampMs <= prevSession.sessionStartTimestampMs) {
                    newSession = mergeSession(newSession, prevSession)

                    updatedSessions -= prevSession
                    newSessions.remove(index - 1)
                    index -= 1
                  }
                }

                newSessions.set(index, newSession)
                updatedSessions += newSession
                updated = true
              }
            }

            if (!updated) {
              // none matched so far, add to last
              newSessions.addLast(event)
              updatedSessions += event
            }
          }

          val newSessionsForScala = newSessions.asScala.toList
          state.update(newSessionsForScala)

          // there must be at least one session available
          // set timeout to earliest sessions' session end: we will traverse and evict sessions
          state.setTimeoutTimestamp(newSessionsForScala.head.sessionEndTimestampMs)

          outputMode match {
            case s if s == OutputMode.Update() =>
              updatedSessions.iterator.map(si =>
                SessionUpdate(sessionId, si.sessionStartTimestampMs / 1000,
                  si.sessionEndTimestampMs / 1000, si.durationMs / 1000, si.numEvents))
            case s if s == OutputMode.Append() => Seq.empty[SessionUpdate].iterator
            case s => throw new UnsupportedOperationException(s"Not supported output mode $s")
          }
        }

        if (state.hasTimedOut) {
          handleEvict(sessionId, state)
        } else {
          handleEvents(sessionId, events, state)
        }
      }

    stateFunc
  }
}
