package com.me.spark.twitter.jobs

import com.me.spark.twitter.config.ApplicationConfig._
import com.me.spark.twitter.config.SparkConfig
import com.me.spark.twitter.utils.PredictionMetrics
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{LogisticRegressionModel, NaiveBayesModel}
import org.apache.spark.sql.DataFrame

/**
  * Reads the test_tweets.csv file and predicts the result
  * This prediction is compared with an already predicted result for determining accuracy
  * We run both the naive bayes and logistic regression classifier and compare their results.
  */
object HateSpeechRecognitionSentiment140 extends SparkConfig {

  val testDf = ImportFromCSV.readSentiment140Csv(sentiment140DatasetTest, ImportFromCSV.sentiment140Schema)

  def main(args: Array[String]): Unit = {
    val preprocessingModel = PipelineModel.load(preprocessedModelPath + "/" + dataSet3)
    val preprocessedDf = preprocessingModel.transform(testDf).cache()

//    println("******* Naive Bayes classification ***************")
//    val nbPredictions = classifyWithNaiveBayesModel(preprocessedDf)
//    printPredictionMetrics(nbPredictions, "nb_result_metrics")
//    println("prediction result will be saved in  " + nbPredictionsPath)
//    writePredictionResultsToFileSystem(nbPredictions, nbPredictionsPath)
//    println()

    println("******** Logistic Regression classification **********")
    val lrPredictions = classifyWithLogisticRegression(preprocessedDf)
    //printPredictionMetrics(lrPredictions, "lr_result_metrics")
    println("prediction result will be saved in  " + lrPredictionsPath)
    //writePredictionResultsToFileSystem(lrPredictions, lrPredictionsPath)
    PredictionMetrics.printAllMetrics(lrPredictions, "lr_sentiment140_test.metrics")
  }

  def classifyWithNaiveBayesModel(preprocessedDf: DataFrame) = {
    val naiveBayesModel = NaiveBayesModel.load(nbModelPath + "/" + dataSet1)
    naiveBayesModel.transform(preprocessedDf)
  }

  def classifyWithLogisticRegression(preprocessedDf: DataFrame) = {
    val lrModel = LogisticRegressionModel.load(lrModelPath + "/" + dataSet3)
    lrModel.transform(preprocessedDf)
  }

  def printPredictionMetrics(df: DataFrame, fileName: String) = {
    val predictionResult = ImportFromCSV.readCSV(resultDS1, ImportFromCSV.tweetSchemaForResult)
    val finalDf = predictionResult.join(df, "id")

    PredictionMetrics.predictionAccuracy(finalDf)
    PredictionMetrics.confusionMatrix(finalDf)
  }

  def writePredictionResultsToFileSystem(df: DataFrame, fileSystemPath: String) = {
    df.select("id", "tweet", "prediction").write.csv(fileSystemPath)
  }
}

