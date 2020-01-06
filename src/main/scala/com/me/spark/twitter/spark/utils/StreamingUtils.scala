package com.me.spark.twitter.spark.utils

object StreamingUtils {

  /**
    * Sets up twitter authentication to read twitter data
    */
  def setupTwitter() = {
    import scala.io.Source

    for (line <- Source.fromFile(this.getClass.getClassLoader.getResource("twitter.txt").getFile).getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }

  /**
    * Set up logging to remove the flooding of unnecessary log messages on the console.
    */
  def setupLogging() = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
