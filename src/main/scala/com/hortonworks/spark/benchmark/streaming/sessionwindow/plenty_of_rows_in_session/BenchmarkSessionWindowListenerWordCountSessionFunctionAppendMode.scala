package com.hortonworks.spark.benchmark.streaming.sessionwindow.plenty_of_rows_in_session

import org.apache.spark.sql.streaming.OutputMode

class BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args: Array[String])
  extends BaseBenchmarkSessionWindowListenerWordCountSessionFunction(
    args,
    "SessionWindowWordCountAsSessionFunctionAppendMode_PlentyOfRowsInSession",
    "SessionWindowWordCountAsSessionFunctionAppendMode_PlentyOfRowsInSession",
    OutputMode.Append())

object BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode {
  def main(args: Array[String]): Unit = {
    new BenchmarkSessionWindowListenerWordCountSessionFunctionAppendMode(args).runBenchmark()
  }

}