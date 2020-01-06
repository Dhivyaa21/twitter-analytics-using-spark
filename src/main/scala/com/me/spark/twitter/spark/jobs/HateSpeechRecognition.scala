package com.me.spark.twitter.spark.jobs

import com.me.spark.twitter.spark.config.ApplicationConfig._
import com.me.spark.twitter.spark.config.SparkConfig
import com.me.spark.twitter.spark.utils.PredictionMetrics
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{LogisticRegressionModel, NaiveBayesModel}
import org.apache.spark.sql.DataFrame

/**
  * Reads the test_tweets.csv file and predicts the result
  * This prediction is compared with an already predicted result for determining accuracy
  * We run both the naive bayes and logistic regression classifier and compare their results.
  */
object HateSpeechRecognition extends SparkConfig {

  val testDf = ImportFromCSV.readCSV(testDS, ImportFromCSV.tweetSchemaForTest)

  def main(args: Array[String]): Unit = {
    val preprocessingModel = PipelineModel.load(preprocessedModelPath)
    val preprocessedDf = preprocessingModel.transform(testDf).cache()

    println("******* Naive Bayes classification ***************")
    val nbPredictions = classifyWithNaiveBayesModel(preprocessedDf)
    printPredictionMetrics(nbPredictions)
    println("prediction result will be saved in  " + nbPredictionsPath)
    writePredictionResultsToFileSystem(nbPredictions, nbPredictionsPath)
    println()

    println("******** Logistic Regression classification **********")
    val lrPredictions = classifyWithLogisticRegression(preprocessedDf)
    printPredictionMetrics(lrPredictions)
    println("prediction result will be saved in  " + lrPredictionsPath)
    writePredictionResultsToFileSystem(lrPredictions, lrPredictionsPath)
  }

  def classifyWithNaiveBayesModel(preprocessedDf: DataFrame) = {
    val naiveBayesModel = NaiveBayesModel.load(nbModelPath)
    naiveBayesModel.transform(preprocessedDf)
  }

  def classifyWithLogisticRegression(preprocessedDf: DataFrame) = {
    val lrModel = LogisticRegressionModel.load(lrModelPath)
    lrModel.transform(preprocessedDf)
  }

  def printPredictionMetrics(df: DataFrame) = {
    val predictionResult = ImportFromCSV.readCSV(resultDS, ImportFromCSV.tweetSchemaForResult)
    val finalDf = predictionResult.join(df, "id")

    PredictionMetrics.printMetrics(finalDf)
    PredictionMetrics.printConfusionMatrix(finalDf)
  }

  def writePredictionResultsToFileSystem(df: DataFrame, fileSystemPath: String) = {
    df.select("id", "tweet", "prediction").write.csv(fileSystemPath)
  }
}

