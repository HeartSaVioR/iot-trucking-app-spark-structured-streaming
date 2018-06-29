package com.hortonworks.spark.utils

import java.util.Properties

import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.streaming.StreamingQueryListener

class QueryListenerSendingProgressToKafka(brokerServers: String, queryStatusTopic: String) extends StreamingQueryListener {
  val logger = LogFactory.getLog(classOf[QueryListenerSendingProgressToKafka].getName)

  val props = new Properties
  props.put("bootstrap.servers", brokerServers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val kafkaProducer = new KafkaProducer[String, String](props)

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logger.info(s"Query is started for ID ${event.id} and RUNID ${event.runId}")

  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {

    try {
      val data = new ProducerRecord[String, String](
        queryStatusTopic, event.progress.runId.toString, event.progress.json)
      kafkaProducer.send(data)
    } catch {
      case e: Exception =>
        logger.error("Error sending event[" + event.progress.json + "] to Kafka topic", e)
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logger.info(s"Query is terminated for ID ${event.id} and RUNID ${event.runId}")
    kafkaProducer.close()
  }
}