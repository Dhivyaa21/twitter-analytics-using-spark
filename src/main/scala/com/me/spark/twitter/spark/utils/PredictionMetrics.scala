package com.me.spark.twitter.spark.utils

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame

object PredictionMetrics {

  def printMetrics(predictedDf: DataFrame) = {
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator()
    evaluator.setPredictionCol("prediction")
    evaluator.setLabelCol("label")
    evaluator.setMetricName("accuracy")
    println("Accuracy - " + evaluator.evaluate(predictedDf))
  }

  def printConfusionMatrix(predictedDf: DataFrame) = {
    println("Confusion Matrix")
    predictedDf.groupBy("label", "prediction").count().show()
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

}
