package com.me.spark.twitter.spark.jobs

import com.me.spark.twitter.spark.config.ApplicationConfig._
import com.me.spark.twitter.spark.config.SparkConfig
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
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

  def readCSV(csvFilePath: String, schema: StructType)(implicit sparkSession: SparkSession) = {
    sparkSession.read
      .option("header", true)
      .option("charset", "ISO-8859-1")
      .schema(schema)
      .csv(csvFilePath)
  }

  def readTextFromCSV(implicit sparkSession: SparkSession) = {
    sparkSession.read
      .textFile(trainingDS)
  }

  def printDataframe(dataframe: DataFrame, take: Int) = {
    dataframe.take(take).foreach(println(_))
  }

  def printCount(dataFrame: DataFrame): Unit = {
    println(dataFrame.count())
  }
}
