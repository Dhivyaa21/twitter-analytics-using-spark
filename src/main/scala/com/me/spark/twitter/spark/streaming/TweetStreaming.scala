package com.me.spark.twitter.spark.streaming

import com.me.spark.twitter.spark.config.ApplicationConfig._
import com.me.spark.twitter.spark.utils.StreamingUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TweetStreaming {

  def main(args: Array[String]): Unit = {
    StreamingUtils.setupTwitter()

    val sc = new StreamingContext("local[*]", "RealTimeHateSpeechPrediction", Seconds(5))
    val sparkSession = SparkSession.builder()
      .appName("RealTimeHateSpeechPrediction")
      .config("spark.sql.warehouse.dir", "file://~/tmp")
      .master("local[*]")
      .getOrCreate()

    val preProcessingModel = PipelineModel.load(preprocessedModelPath)
    val lrModel = LogisticRegressionModel.load(lrModelPath)

    StreamingUtils.setupLogging()

    val tweets = TwitterUtils.createStream(sc, None)
    val englishTweets = tweets.filter(_.getLang.equalsIgnoreCase("en")).map(t => Tweets(t.getId.toInt, t.getText))
    englishTweets.foreachRDD((rdd, time) => {
      if (rdd.count() > 0 ) {
        val sQLContext = sparkSession.sqlContext
        import sQLContext.implicits._
        val filteredTweets = rdd.toDF("id", "tweet")
        val preprocessedDf = preProcessingModel.transform(filteredTweets)
        val predictions = lrModel.transform(preprocessedDf)
        predictions.select("tweet", "prediction").show(false)
      }
      else {
        println("stream empty")
      }
    })

    sc.checkpoint("~/checkpoint")
    sc.start()
    sc.awaitTerminationOrTimeout(60000)
  }

  case class Tweets(id: Int, tweet: String)

}
