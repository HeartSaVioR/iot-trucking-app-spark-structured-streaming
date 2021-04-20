package com.hortonworks.spark.benchmark.streaming.kafka

import java.io.File
import java.nio.file.Files
import java.util.UUID

import com.hortonworks.spark.utils.QueryListenerWriteProgressToFile
import org.apache.spark.sql.SparkSession

object KafkaProducerBenchmarkRunner {
  def runBenchmark(conf: KafkaProducerBenchmarkAppConf): Unit = {
    val appName = "kafka-producer-full-speed-benchmark"

    val rateRowPerSecond = conf.rateRowPerSecond()
    val rateRampUpTimeSecond = conf.rateRampUpTimeSecond()

    val ss = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    conf.queryStatusFile.foreach { file =>
      ss.streams.addListener(new QueryListenerWriteProgressToFile(file))
    }

    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", rateRowPerSecond)
      .option("rampUpTime", s"${rateRampUpTimeSecond}s")
      .option("numPartitions", conf.numPartitions())
      .load()

    df.printSchema()

    val checkpointDir = conf.checkpointDirectory.getOrElse(
      Files.createTempDirectory("ss-kafka-producer-benchmark").toFile.getAbsolutePath)

    val query = df
      .selectExpr("CAST(value as STRING) as value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.kafkaBootstrapServers())
      .option("topic", conf.kafkaOutputTopic())
      .option("checkpointLocation", checkpointDir)
      .start()

    query.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val conf = KafkaProducerBenchmarkAppConf.create(args)
    KafkaProducerBenchmarkRunner.runBenchmark(conf)
  }
}
