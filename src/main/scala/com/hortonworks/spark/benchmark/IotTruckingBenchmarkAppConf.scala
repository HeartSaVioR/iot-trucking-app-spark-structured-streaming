package com.hortonworks.spark.benchmark

import org.apache.spark.sql.streaming.OutputMode
import org.rogach.scallop.ScallopConf

abstract class IotTruckingBenchmarkAppConf(args: Array[String]) extends ScallopConf(args) {
  import IotTruckingBenchmarkAppConf._

  val queryStatusFile = opt[String](name = "query-status-file", required = false, noshort = true)
  val rateRowPerSecond = opt[String](name = "rate-row-per-second", required = true, noshort = true)
  val rateRampUpTimeSecond = opt[String](name = "rate-ramp-up-time-second", required = true, noshort = true)
  val checkpointDirectory = opt[String](name = "checkpoint-dir", required = false, noshort = true)

  val outputMode = opt[String](name = "output-mode", required = true, noshort = true,
    validate = (s: String) => validOutputModes.map(_.toLowerCase()).contains(s.toLowerCase()))

  def getSparkOutputMode: OutputMode = {
    outputMode.toOption match {
      case Some("append") => OutputMode.Append()
      case Some("update") => OutputMode.Update()
      case Some("complete") => OutputMode.Complete()
      case Some(x) => throw new IllegalArgumentException(s"Not supported output mode: $x")
      case None => throw new IllegalArgumentException("Output mode must be presented!")
    }
  }
}

object IotTruckingBenchmarkAppConf {
  val DEFAULT_GEO_EVENTS_TOPIC = "truck_events_stream"
  val DEFAULT_SPEED_EVENTS_TOPIC = "truck_speed_events_stream"
  val DEFAULT_APP_QUERY_STATUS_TOPIC = "app_query_progress"

  val validOutputModes = Seq("append", "update", "complete")
}
