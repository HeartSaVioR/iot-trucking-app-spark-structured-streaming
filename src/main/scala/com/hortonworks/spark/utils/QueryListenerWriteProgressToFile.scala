package com.hortonworks.spark.utils

import java.io.{File, PrintWriter}

import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.streaming.StreamingQueryListener

class QueryListenerWriteProgressToFile(queryStatusFile: String) extends StreamingQueryListener {
  val logger = LogFactory.getLog(classOf[QueryListenerWriteProgressToFile].getName)
  val writer = new PrintWriter(new File(queryStatusFile))

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    logger.info(s"Query is started for ID ${event.id} and RUNID ${event.runId}")
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {

    try {
      writer.write(event.progress.json + "\n")
      writer.flush()
    } catch {
      case e: Exception =>
        logger.error("Error write event[" + event.progress.json + "] to file", e)
    }
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logger.info(s"Query is terminated for ID ${event.id} and RUNID ${event.runId}")
    writer.close()
  }
}