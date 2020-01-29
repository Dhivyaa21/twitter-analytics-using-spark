package com.me.spark.twitter.jobs.sparkml

import com.me.spark.twitter.config.ApplicationConfig
import com.me.spark.twitter.jobs.sparkml.classifiers.Classifier
import com.me.spark.twitter.preprocessing.TweetPreprocessorForDS2
import com.me.spark.twitter.utils.PredictionMetrics
import org.apache.spark.sql.DataFrame

object Dataset2Classifier extends Classifier {
  override val preprocessedDf: DataFrame = {
    TweetPreprocessorForDS2.getPreprocessedTweets()
  }
  override val nbModelPath: String = ApplicationConfig.nbModelPath + '/' + ApplicationConfig.dataSet2
  override val lrModelPath: String = ApplicationConfig.lrModelPath + '/' + ApplicationConfig.dataSet2

  def main(args: Array[String]): Unit = {
    val nbPredictions = trainWithNaiveBayesModel()
    PredictionMetrics.printAllMetrics(nbPredictions, "nb_dataset2.metrics")
    val lrPredictions = trainWithLogisticRegressionModel()
    PredictionMetrics.printAllMetrics(lrPredictions, "lr_dataset2.metrics")
  }
}
