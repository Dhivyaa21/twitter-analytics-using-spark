package com.me.spark.twitter.streaming

import com.me.spark.twitter.config.ApplicationConfig._
import com.me.spark.twitter.kafka.KafkaConfig
import com.me.spark.twitter.utils.StreamingUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{LogisticRegressionModel, NaiveBayesModel}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TweetStreaming {

  def main(args: Array[String]): Unit = {
    StreamingUtils.setupTwitter()

    val sc = new StreamingContext("local[*]", "RealTimeHateSpeechPrediction", Seconds(5))
    implicit val sparkSession = SparkSession.builder()
      .appName("RealTimeHateSpeechPrediction")
      .config("spark.sql.warehouse.dir", "file://~/tmp")
      .master("local[*]")
      .getOrCreate()

    val preProcessingModel = PipelineModel.load(preprocessedModelPath + '/' + dataSet3)
    val lrModel = LogisticRegressionModel.load(lrModelPath + '/' + dataSet3)
    val nbModel = NaiveBayesModel.load(nbModelPath + "/" + dataSet3)

    StreamingUtils.setupLogging()

    val tweets = TwitterUtils.createStream(sc, None)
    val englishTweets = tweets
      .filter(_.getLang.equalsIgnoreCase("en"))
      .map(t => Tweets(t.getId.toInt, t.getText))
    englishTweets.foreachRDD((rdd, time) => {
      if (rdd.count() > 0) {
        val sQLContext = sparkSession.sqlContext
        import sQLContext.implicits._
        val filteredTweets = rdd.toDF("id", "tweet")
        val preprocessedDf = preProcessingModel.transform(filteredTweets)
        val predictions = lrModel.transform(preprocessedDf)
        import org.apache.spark.sql.functions.udf
        def classToString = udf((prediction: Int) => {
          if (prediction == 0) "negative"
          else if (prediction == 2) "neutral"
          else if (prediction == 4) "positive"
          else "invalid prediction"
        })

        //Publish the results to kafka
        KafkaConfig.publishToKafka(predictions.withColumnRenamed("id", "tweetId"))

        //Subscribe to kafka to post the results to a visualization tool.
        //KafkaConfig.subscribeToKafka()
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

  case class TweetsWithLocation(id: Int, tweet: String, latitude: Double, longitude: Double)

}
