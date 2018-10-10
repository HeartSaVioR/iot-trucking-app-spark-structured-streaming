package com.hortonworks.spark.benchmark.streaming.sessionwindow

import org.apache.spark.sql.streaming.OutputMode

class BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListenerWordCountSessionFunction(
    args,
    "SessionWindowWordCountAsSessionFunctionAppendMode",
    "SessionWindowWordCountAsSessionFunctionAppendMode",
    OutputMode.Append())

object BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode {
  def main(args: Array[String]): Unit = {
    new BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args).runBenchmark()
  }

}