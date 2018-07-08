package com.hortonworks.spark

import com.hortonworks.spark.utils.{IotTruckingAppConf, QueryListenerSendingProgressToKafka}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object BenchmarkMovingAggregations {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val conf = new IotTruckingAppConf(args)
    val brokers = conf.brokers()

    val ss = SparkSession
      .builder()
      .appName("BenchmarkMovingAggregations")
      .getOrCreate()

    conf.queryStatusTopic.foreach(topic =>
      ss.streams.addListener(new QueryListenerSendingProgressToKafka(brokers, topic)))

    import org.apache.spark.sql.functions._
    import ss.implicits._

    val df = ss.readStream
      .format("rate")
      .option("rowsPerSecond", "10000")
      .load()

    df.printSchema()

    val outDf = df
      .withWatermark("timestamp", "10 seconds")
      .selectExpr(
      "timestamp", "mod(value, 100) as mod", "value",
        createCaseExprStr(50, "mod(CAST(RANDN(0) * 1000 as INTEGER), 50)") + " as word")
      .groupBy(
        window($"timestamp", "1 minute", "10 seconds"),
        $"mod", $"word")
      .agg(max("value").as("max_value"), min("value").as("min_value"), avg("value").as("avg_value"))

    val query = outDf
      .writeStream
      .format("memory")
      .option("checkpointLocation", conf.checkpoint())
      .option("queryName", "movingAggregation")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

  def createCaseExprStr(cnt: Int, valExpr: String): String = {
    require(words.length >= cnt)

    var caseExpr = s"CASE $valExpr "
    words.take(cnt).zipWithIndex.foreach {
      case (word, idx) => caseExpr += s"WHEN $idx THEN '$word' "
    }

    caseExpr += "ELSE 'unpleasant' END "

    caseExpr
  }

  val words: Array[String] = Array(
    "connection",
    "correspond",
    "preference",
    "relinquish",
    "exaggerate",
    "decoration",
    "philosophy",
    "attractive",
    "distribute",
    "protection",
    "homosexual",
    "assumption",
    "reluctance",
    "motorcycle",
    "decorative",
    "acceptance",
    "litigation",
    "democratic",
    "accessible",
    "indication",
    "commitment",
    "concession",
    "hypnothize",
    "obligation",
    "dependence",
    "conscience",
    "engagement",
    "plagiarize",
    "artificial",
    "initiative",
    "depression",
    "memorandum",
    "censorship",
    "resolution",
    "settlement",
    "prevalence",
    "enthusiasm",
    "investment",
    "simplicity",
    "commission",
    "corruption",
    "population",
    "tournament",
    "reasonable",
    "restaurant",
    "provincial",
    "assessment",
    "continuous",
    "conclusion",
    "appreciate",
    "foundation",
    "television",
    "admiration",
    "stereotype",
    "psychology",
    "distortion",
    "disability",
    "presidency",
    "retirement",
    "assignment",
    "transition",
    "reputation",
    "overcharge",
    "discipline",
    "motivation",
    "understand",
    "repetition",
    "production",
    "generation",
    "break down",
    "wilderness",
    "management",
    "reflection",
    "diplomatic",
    "basketball",
    "mainstream",
    "cell phone",
    "leadership",
    "photograph",
    "conception",
    "particular",
    "federation",
    "separation",
    "houseplant",
    "chimpanzee",
    "acceptable",
    "conference",
    "systematic",
    "remunerate",
    "excavation",
    "functional",
    "occupation",
    "illustrate",
    "negligence",
    "laboratory",
    "deficiency",
    "mastermind",
    "withdrawal",
    "goalkeeper",
    "first-hand")

}
