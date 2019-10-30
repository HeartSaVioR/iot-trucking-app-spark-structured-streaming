package com.hortonworks.spark.benchmark.streaming.kafka

import com.hortonworks.spark.benchmark.IotTruckingBenchmarkAppConf

class KafkaProducerBenchmarkAppConf(args: Array[String]) extends IotTruckingBenchmarkAppConf(args) {
  import KafkaProducerBenchmarkAppConf._

  val numPartitions = opt[Int](name = "num-partitions", required = true, noshort = true)

  val kafkaBootstrapServers = opt[String](name = "bootstrap-servers", required = true, noshort = true)

  val kafkaOutputTopic = opt[String](name = "output-topic", required = true, noshort = true)

  verify()
}

object KafkaProducerBenchmarkAppConf {
  def create(args: Array[String]): KafkaProducerBenchmarkAppConf = {
    new KafkaProducerBenchmarkAppConf(args) {()}
  }
}