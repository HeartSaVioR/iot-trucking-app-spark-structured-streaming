package com.hortonworks.spark.experiment

import java.sql.Timestamp

import com.hortonworks.spark.utils.QueryListenerWriteProgressToFile
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, Trigger}

case class RunningCount(count: Long)

object TestFlatMapGroupsWithState {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val ss = SparkSession
      .builder()
      .appName("TestFlatMapGroupsWithState")
      .getOrCreate()

    ss.conf.set("spark.sql.shuffle.partitions", "5")

    import ss.implicits._

    ss.streams.addListener(new QueryListenerWriteProgressToFile("/tmp/query-status"))

    val stateFunc = (key: String, values: Iterator[String], state: GroupState[RunningCount]) => {
      if (state.hasTimedOut) {
        // End users are not restricted to remove the state here - they can update the
        // state as well. For example, event time session window would have list of
        // sessions here and it cannot remove entire state.
        state.update(RunningCount(-1))
        Iterator((key, "-1"))
      } else {
        val count = state.getOption.map(_.count).getOrElse(0L) + values.size
        state.update(RunningCount(count))
        state.setTimeoutDuration("1 seconds")
        Iterator((key, count.toString))
      }
    }

    implicit val sqlContext = ss.sqlContext
    val inputData = MemoryStream[String]

    val result = inputData
      .toDF()
      .as[String]
      .groupByKey { v => v }
      .flatMapGroupsWithState(OutputMode.Append(), GroupStateTimeout.ProcessingTimeTimeout())(stateFunc)

    val query = result
      .writeStream
      .format("memory")
      .option("queryName", "test")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("5 second"))
      .start()

    Thread.sleep(1000)

    var chIdx: Long = 0

    while (true) {
      (chIdx to chIdx + 4).map { idx => inputData.addData(idx.toString) }
      chIdx += 5
      Thread.sleep(10 * 1000)
    }
  }
}
