package com.hortonworks.spark.benchmark.streaming.sessionwindow

import com.hortonworks.spark.benchmark.IotTruckingBenchmarkAppConf
import org.apache.spark.sql.streaming.OutputMode

class SessionWindowBenchmarkAppConf(args: Array[String]) extends IotTruckingBenchmarkAppConf(args) {
  import SessionWindowBenchmarkAppConf._

  val method = opt[String](name = "method", required = true, noshort = true,
    validate = (s: String) => validMethods.map(_.toLowerCase()).contains(s.toLowerCase()))

  val dataPattern = opt[String](name = "data-pattern", required = true, noshort = true,
    validate = (s: String) => validDataPatterns.map(_.toLowerCase()).contains(s.toLowerCase()))

  verify()

  def getMethod: String = method.toOption match {
    case Some(x) => x
    case None => throw new IllegalArgumentException("method must be specified!")
  }

  def getDataPattern: String = dataPattern.toOption match {
    case Some(x) => x
    case None => throw new IllegalArgumentException("data pattern must be specified!")
  }

  def initializeBenchmarkClass: BaseBenchmarkSessionWindowListener = {
    val outMode = getSparkOutputMode
    val me = getMethod
    val dataPattern = getDataPattern

    (me.toUpperCase(), dataPattern.toUpperCase()) match {
        // flatMapGroupsWithState & PlentyOfKeys
        // FIXME: not verified work
      case (m, d) if m == methodFlatMapGroupsWithState.toUpperCase() && d == dataPatternPlentyOfKeys.toUpperCase() =>
        new plenty_of_keys.BenchmarkSessionWindowListenerWordCountFlatMapGroupsWithState(
          this, createQueryName(outMode, me, dataPattern), createQueryName(outMode, me, dataPattern))

      // flatMapGroupsWithState & PlentyOfSessionsInKeys
      // FIXME: not verified work
      case (m, d) if m == methodFlatMapGroupsWithState.toUpperCase() && d == dataPatternPlentyOfSessionsInKeys.toUpperCase() =>
        new plenty_of_sessions_in_key.BenchmarkSessionWindowListenerWordCountFlatMapGroupsWithState(
          this, createQueryName(outMode, me, dataPattern), createQueryName(outMode, me, dataPattern))

      // flatMapGroupsWithState & PlentyOfRowsInSession
      // FIXME: not verified work
      case (m, d) if m == methodFlatMapGroupsWithState.toUpperCase() && d == dataPatternPlentyOfRowsInSession.toUpperCase() =>
        new plenty_of_rows_in_session.BenchmarkSessionWindowListenerWordCountFlatMapGroupsWithState(
          this, createQueryName(outMode, me, dataPattern), createQueryName(outMode, me, dataPattern))

        // methodSessionWindow & PlentyOfKeys
      // FIXME: not verified work
      case (m, d) if m == methodSessionWindow.toUpperCase() && d == dataPatternPlentyOfKeys.toUpperCase() =>
        new plenty_of_keys.BenchmarkSessionWindowListenerWordCountSessionFunction(
          this, createQueryName(outMode, me, dataPattern), createQueryName(outMode, me, dataPattern))

      // methodSessionWindow & PlentyOfSessionsInKeys
      // FIXME: not verified work
      case (m, d) if m == methodSessionWindow.toUpperCase() && d == dataPatternPlentyOfSessionsInKeys.toUpperCase() =>
        new plenty_of_sessions_in_key.BenchmarkSessionWindowListenerWordCountSessionFunction(
          this, createQueryName(outMode, me, dataPattern), createQueryName(outMode, me, dataPattern))

      // methodSessionWindow & PlentyOfRowsInSession
      // FIXME: not verified work
      case (m, d) if m == methodSessionWindow.toUpperCase() && d == dataPatternPlentyOfRowsInSession.toUpperCase() =>
        new plenty_of_rows_in_session.BenchmarkSessionWindowListenerWordCountSessionFunction(
          this, createQueryName(outMode, me, dataPattern), createQueryName(outMode, me, dataPattern))

      case _ => throw new IllegalArgumentException(s"Unknown pair of inputs - method $me / data pattern $dataPattern")
    }
  }

  def createQueryName(outputMode: OutputMode, method: String, dataPattern: String): String = {
    s"SessionWindowWordCountAs${method}_${outputMode.toString}_$dataPattern"
  }
}

object SessionWindowBenchmarkAppConf {
  val methodFlatMapGroupsWithState = "FlatmapGroupsWithState"
  val methodSessionWindow = "SessionWindow"
  val validMethods = Seq(methodFlatMapGroupsWithState, methodSessionWindow)

  val dataPatternPlentyOfKeys = "Plenty_Of_Keys"
  val dataPatternPlentyOfSessionsInKeys = "Plenty_Of_Sessions_In_Key"
  val dataPatternPlentyOfRowsInSession = "Plenty_Of_Rows_In_Session"
  val validDataPatterns = Seq(dataPatternPlentyOfKeys, dataPatternPlentyOfRowsInSession,
    dataPatternPlentyOfSessionsInKeys)

  def create(args: Array[String]): SessionWindowBenchmarkAppConf = {
    new SessionWindowBenchmarkAppConf(args) {()}
  }
}