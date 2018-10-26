package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_keys

import org.apache.spark.sql.streaming.OutputMode

class BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListenerWordCountSessionFunction(
    args,
    "SessionWindowWordCountAsSessionFunctionAppendMode_PlentyOfKeys",
    "SessionWindowWordCountAsSessionFunctionAppendMode_PlentyOfKeys",
    OutputMode.Append())

object BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode {
  def main(args: Array[String]): Unit = {
    new BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args).runBenchmark()
  }

}