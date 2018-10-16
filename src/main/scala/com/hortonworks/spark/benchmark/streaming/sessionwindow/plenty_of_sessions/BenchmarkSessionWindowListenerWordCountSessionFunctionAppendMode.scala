package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_sessions

import org.apache.spark.sql.streaming.OutputMode

class BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListenerWordCountSessionFunction(
    args,
    "SessionWindowWordCountAsSessionFunctionAppendMode_PlentyOfSessions",
    "SessionWindowWordCountAsSessionFunctionAppendMode_PlentyOfSessions",
    OutputMode.Append())

object BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode {
  def main(args: Array[String]): Unit = {
    new BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args).runBenchmark()
  }

}