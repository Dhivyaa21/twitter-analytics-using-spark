package com.me.spark.twitter.jobs.sparkml.classifiers

import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.sql.DataFrame

trait Classifier {

  val preprocessedDf: DataFrame
  val nbModelPath: String
  val lrModelPath: String

  def trainWithNaiveBayesModel() = {
    println("********** Naive Bayes Classification *************")
    val nbClassifier = new NaiveBayes()
    nbClassifier.setLabelCol("label")
    nbClassifier.setFeaturesCol("features")
    nbClassifier.setPredictionCol("prediction")

    val nbModel = nbClassifier.fit(preprocessedDf)
    nbModel.write.overwrite().save(nbModelPath)
    nbModel.transform(preprocessedDf)
  }

  def trainWithLogisticRegressionModel() = {
    println("********** Logistic regression Classification *************")
    val lrClassifier = new LogisticRegression()
      .setFamily("multinomial")
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)

    val lrModel = lrClassifier.fit(preprocessedDf)
    lrModel.write.overwrite().save(lrModelPath)
    lrModel.transform(preprocessedDf)
  }
}
