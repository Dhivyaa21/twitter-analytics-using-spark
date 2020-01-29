package com.me.spark.twitter.sparkml

import com.me.spark.twitter.config.ApplicationConfig
import com.me.spark.twitter.preprocessing.TweetPreprocessorForSentiment140
import com.me.spark.twitter.sparkml.classifiers.Classifier
import com.me.spark.twitter.utils.PredictionMetrics
import org.apache.spark.sql.DataFrame

object Sentiment140TweetsClassifier extends Classifier {
  override val preprocessedDf: DataFrame = TweetPreprocessorForSentiment140.getPreprocessedTweets()
  override val nbModelPath: String = ApplicationConfig.nbModelPath + "/" + ApplicationConfig.dataSet3
  override val lrModelPath: String = ApplicationConfig.lrModelPath + "/" + ApplicationConfig.dataSet3

  def main(args: Array[String]): Unit = {
    val nbPredictions = trainWithNaiveBayesModel()
    PredictionMetrics.printAllMetrics(nbPredictions, "nb_sentiment140.metrics")
    val lrPredictions = trainWithLogisticRegressionModel()
    PredictionMetrics.printAllMetrics(lrPredictions, "lr_sentiment140.metrics")
  }
}
