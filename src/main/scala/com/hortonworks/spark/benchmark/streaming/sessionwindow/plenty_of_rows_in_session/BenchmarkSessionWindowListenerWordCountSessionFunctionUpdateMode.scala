package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_rows_in_session

import org.apache.spark.sql.streaming.OutputMode

class BenchmarkSessionWindowListenerWordCountSessionFunctionUpdateMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListenerWordCountSessionFunction(
    args,
    "SessionWindowWordCountAsSessionFunctionUpdateMode_PlentyOfRowsInSession",
    "SessionWindowWordCountAsSessionFunctionUpdateMode_PlentyOfRowsInSession",
    OutputMode.Update())

object BenchmarkSessionWindowListenerWordCountSessionFunctionUpdateMode {
  def main(args: Array[String]): Unit = {
    new BenchmarkSessionWindowListenerWordCountSessionFunctionUpdateMode(args).runBenchmark()
  }

}