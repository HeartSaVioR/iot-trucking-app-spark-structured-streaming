package com.hortonworks.spark.benchmark.streaming.sessionwindow

object SessionWindowBenchmarkRunner {
  def main(args: Array[String]): Unit = {
    val conf = SessionWindowBenchmarkAppConf.create(args)

    val benchmarkInstance = conf.initializeBenchmarkClass

    println(s"Benchmark class: ${benchmarkInstance.getClass.getCanonicalName}")
    println(s"Output Mode: ${conf.getSparkOutputMode}")
    println(s"Method: ${conf.getMethod}")
    println(s"Data Pattern: ${conf.getDataPattern}")

    benchmarkInstance.runBenchmark()
  }

}
