package com.me.spark.twitter.jobs

import com.me.spark.twitter.config.ApplicationConfig._
import com.me.spark.twitter.config.SparkConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object ImportFromCSV extends SparkConfig {

  val tweetSchemaForTraining = StructType(
    Seq(StructField("id", StringType, false),
      StructField("label", DoubleType, false),
      StructField("tweet", StringType, false))
  )

  val tweetSchemaForTest = StructType(
    Seq(StructField("id", StringType, false),
      StructField("tweet", StringType, false))
  )

  val tweetSchemaForResult = StructType(
    Seq(StructField("id", StringType, false),
      StructField("label", DoubleType, false))
  )

  val tweetSchemaDS2 = StructType(
    Seq(StructField("id", DoubleType, false),
      StructField("count", DoubleType, false),
      StructField("hate_speech", DoubleType, false),
      StructField("offensive_language", DoubleType, false),
      StructField("neither", DoubleType, false),
      StructField("label", DoubleType, false),
      StructField("tweet", StringType, false))
  )

  val sentiment140Schema = StructType(
    Seq(StructField("label", DoubleType, false),
      StructField("id", DoubleType, false),
      StructField("date", StringType, false),
      StructField("flag", StringType, false),
      StructField("user", StringType, false),
      StructField("tweet", StringType, false))
  )

  def readCSV(csvFilePath: String, schema: StructType)(implicit sparkSession: SparkSession) = {
    sparkSession.read
      .option("header", true)
      .option("charset", "ISO-8859-1")
      .schema(schema)
      .csv(csvFilePath)
  }

  def readMultiLineCSV(csvFilePath: String, schema: StructType)(implicit sparkSession: SparkSession) = {
    sparkSession.read
      .option("header", true)
      .option("multiLine", true)
      .option("escape", "\"")
      .option("charset", "ISO-8859-1")
      .schema(schema)
      .csv(csvFilePath)
  }

  def readSentiment140Csv(csvFilePath: String, schema: StructType)(implicit sparkSession: SparkSession) = {
    println("SQL conf - " + sparkSession.conf.get(SQLConf.ORC_IMPLEMENTATION.key))
    sparkSession.read
      .option("header", false)
      .option("charset", "ISO-8859-1")
      .schema(schema)
      .csv(csvFilePath)
  }

  def readTextFromCSV(implicit sparkSession: SparkSession) = {
    sparkSession.read
      .textFile(trainingDS1)
  }

  def printDataframe(dataframe: DataFrame, take: Int) = {
    dataframe.take(take).foreach(println(_))
  }

  def printCount(dataFrame: DataFrame): Unit = {
    println(dataFrame.count())
  }
}
