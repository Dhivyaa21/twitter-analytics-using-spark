package com.me.spark.twitter.utils

import java.io.{File, FileOutputStream}

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame

object PredictionMetrics {

  def predictionAccuracy(predictedDf: DataFrame) = {
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setPredictionCol("prediction")
    evaluator.setLabelCol("label")
    evaluator.setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictedDf)
    accuracy
  }

  def confusionMatrix(predictedDf: DataFrame) = {
    predictedDf.groupBy("label", "prediction").count()
  }

  def printLRMetrics(lrModel: LogisticRegressionModel, predictions: DataFrame) = {
    println("\nIntercept: " + lrModel.intercept)
    println(lrModel.summary)
    val evaluator = new RegressionEvaluator()
    evaluator.setPredictionCol("prediction")
    evaluator.setLabelCol("label")
    evaluator.setMetricName("r2")
    println("\nAccuracy = " + evaluator.evaluate(predictions))
  }

  def printAllMetrics(predictions: DataFrame, fileName: String) = {
    println("Raw predictions")
    predictions.show(false)
    writeMetricsToFile(predictions, fileName)
  }

  def writeMetricsToFile(predictions: DataFrame, fileName: String) = {
    val metricsFile = new File(fileName)
    if (metricsFile.exists()) metricsFile.delete()
    metricsFile.createNewFile()
    val fos = new FileOutputStream(metricsFile)
    Console.withOut(fos) {
      val accuracy = predictionAccuracy(predictions)
      println(s"Accuracy - $accuracy")
      println("Confusion Matrix")
      val confMatrix = confusionMatrix(predictions)
      confMatrix.show(false)
    }
  }
}
