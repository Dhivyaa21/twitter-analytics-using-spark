package com.me.spark.twitter.config

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

/**
  * Loads the spark session object required for the application.
  * Loads the session as an implicit value which can be imported into classes
  */
class SparkConfig {

  val logger = Logger.getLogger(getClass)

  implicit val sparkSession = SparkSession.builder()
    .config(new SparkConf().setMaster("local[*]")
      .set("spark.buffer.pageSize", "2m"))
    .getOrCreate()

}
