package com.hortonworks.spark.benchmark.streaming.timewindow

import com.hortonworks.spark.benchmark.IotTruckingBenchmarkAppConf

class TimeWindowBenchmarkAppConf(args: Array[String]) extends IotTruckingBenchmarkAppConf(args) {
  import TimeWindowBenchmarkAppConf._

  val dataPattern = opt[String](name = "data-pattern", required = true, noshort = true,
    validate = (s: String) => validDataPatterns.map(_.toLowerCase()).contains(s.toLowerCase()))

  verify()

  def getDataPattern: String = dataPattern.toOption match {
    case Some(x) => x
    case None => throw new IllegalArgumentException("data pattern must be specified!")
  }

  def initializeBenchmarkClass: BaseBenchmarkMovingAggregationListener = {
    getDataPattern.toLowerCase() match {
      case d if d == dataPatternNormal.toLowerCase() => new BenchmarkMovingAggregationsListener(this)
      case d if d == dataPatternKeyMuchBigger.toLowerCase() => new BenchmarkMovingAggregationsListenerKeyMuchBigger(this)
      case d if d == dataPatternManyKeys.toLowerCase() => new BenchmarkMovingAggregationsListenerManyKeys(this)
      case d if d == dataPatternManyValues.toLowerCase() => new BenchmarkMovingAggregationsListenerManyValues(this)
      case d if d == dataPatternValueMuchBigger.toLowerCase() => new BenchmarkMovingAggregationsListenerValueMuchBigger(this)
      case _ => throw new IllegalArgumentException(s"Unknown data pattern - $dataPattern")
    }
  }
}

object TimeWindowBenchmarkAppConf {
  val dataPatternNormal = "Normal"
  val dataPatternKeyMuchBigger = "Key_Much_Bigger"
  val dataPatternManyKeys = "Many_Keys"
  val dataPatternManyValues = "Many_Values"
  val dataPatternValueMuchBigger = "Value_Much_Bigger"
  val validDataPatterns = Seq(dataPatternNormal, dataPatternKeyMuchBigger,
    dataPatternManyKeys, dataPatternManyValues, dataPatternValueMuchBigger)

  def create(args: Array[String]): TimeWindowBenchmarkAppConf = {
    new TimeWindowBenchmarkAppConf(args) {()}
  }
}
