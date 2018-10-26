package com.hortonworks.spark.benchmark.streaming.timewindow

object TimeWindowBenchmarkRunner {
  def main(args: Array[String]): Unit = {
    val conf = TimeWindowBenchmarkAppConf.create(args)

    val benchmarkInstance = conf.initializeBenchmarkClass

    println(s"Benchmark class: ${benchmarkInstance.getClass.getCanonicalName}")
    println(s"Output Mode: ${conf.getSparkOutputMode}")

    benchmarkInstance.runBenchmark()
  }

}
