package com.hortonworks.spark.benchmark

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{avg, max, min}

object BenchmarkQueryHelper {

  def createBunchOfColumnExpressions(ss: SparkSession, count: Int, numericExpr: String,
                                     newColumnNamePrefix: String):
  Seq[(String, Column)] = {
    import ss.implicits._
    (1 to count).map(i => (s"($numericExpr + $i) as $newColumnNamePrefix$i",
      $"$newColumnNamePrefix$i"))
  }

  def createBunchOfAggExpressions(columns: Seq[Column]): Seq[Column] = {
    columns.flatMap(col => Seq(max(col), min(col), avg(col)))
  }

  def createCaseExprStr(valExpr: String, count: Int, lengthPerColumn: Int): String = {
    def createStretchedWord(word: String): String = {
      (1 to ((lengthPerColumn / word.length) + 1)).map(_ => word)
        .mkString("").substring(0, lengthPerColumn)
    }

    require(words.length >= count)
    require(lengthPerColumn % 10 == 0)

    var caseExpr = s"CASE $valExpr "
    words
      .take(count - 1)
      .map(createStretchedWord)
      .zipWithIndex.foreach {
      case (word, idx) => caseExpr += s"WHEN $idx THEN '$word' "
    }

    caseExpr += words
      .drop(count - 1)
      .take(1)
      .map(createStretchedWord)
      .map(word => s"ELSE '$word' END ")
      .last

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
    "first-hand",
    "unpleasant")

}
