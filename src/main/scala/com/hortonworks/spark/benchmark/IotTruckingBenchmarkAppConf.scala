package com.hortonworks.spark.benchmark

import org.rogach.scallop.ScallopConf

class IotTruckingBenchmarkAppConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val queryStatusFile = opt[String](name = "query-status-file", required = true, noshort = true)
  val rateRowPerSecond = opt[String](name = "rate-row-per-second", required = true, noshort = true)
  val rateRampUpTimeSecond = opt[String](name = "rate-ramp-up-time-second", required = true, noshort = true)

  verify()
}
