package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_sessions

import org.apache.spark.sql.streaming.OutputMode

class BenchmarkSessionWindowListenerWordCountSessionFunctionUpdateMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListenerWordCountSessionFunction(
      args,
      "SessionWindowWordCountAsSessionFunctionUpdateMode_PlentyOfSessions",
      "SessionWindowWordCountAsSessionFunctionUpdateMode_PlentyOfSessions",
      OutputMode.Update())

object BenchmarkSessionWindowListenerWordCountSessionFunctionUpdateMode {
  def main(args: Array[String]): Unit = {
    new BenchmarkSessionWindowListenerWordCountSessionFunctionUpdateMode(args).runBenchmark()
  }

}