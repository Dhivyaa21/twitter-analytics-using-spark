package com.me.spark.twitter.spark.jobs

import com.me.spark.twitter.spark.config.ApplicationConfig._
import com.me.spark.twitter.spark.config.SparkConfig
import com.me.spark.twitter.spark.jobs.ImportFromCSV.readCSV
import com.me.spark.twitter.spark.preprocessing.TextPreprocessingPipeline
import com.me.spark.twitter.spark.utils.PredictionMetrics
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.sql.DataFrame

/**
  * Train our datasets for hate speech prediction
  * We train our datasets to create both naive bayes and logistic regression model.
  * We compare the accuracy of prediction results of both the models.
  */
object HSRTraining extends SparkConfig {

  val trainDf = readCSV(trainingDS, ImportFromCSV.tweetSchemaForTraining).select("label", "id", "tweet")

  def main(args: Array[String]): Unit = {
    val pipeline = TextPreprocessingPipeline.createPipeline
    val preprocessedModel = pipeline.fit(trainDf)
    preprocessedModel.write.overwrite().save(preprocessedModelPath)
    val preprocessedDf = preprocessedModel.transform(trainDf).cache()

    println("********** Naive Bayes Classification *************")
    val nbPredictions = trainWithNaiveBayesModel(preprocessedDf)
    printMetrics(nbPredictions)

    println()
    println("********** Logistic Regression classification *************")
    val lrPredictions = trainWithLogisticRegressionModel(preprocessedDf)
    printMetrics(lrPredictions)

    /**
      * Kmeans clustering can be used to balance the datasets.
      */
    //KMeansClustering.balanceDataframe(nonHateDf, hateDf.count().toInt)
    //val balancedDf = underSample(preprocessedDf)
    //balancedDf.select("label", "features").show(10)
  }

  def trainWithNaiveBayesModel(preprocessedDf: DataFrame) = {
    val nbClassifier = new NaiveBayes()
    nbClassifier.setLabelCol("label")
    nbClassifier.setFeaturesCol("features")
    nbClassifier.setPredictionCol("prediction")

    val nbModel = nbClassifier.fit(preprocessedDf)
    nbModel.write.overwrite().save(nbModelPath)
    nbModel.transform(preprocessedDf)
  }

  def trainWithLogisticRegressionModel(preprocessedDf: DataFrame) = {
    val lrClassifier = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)

    val lrModel = lrClassifier.fit(preprocessedDf)
    lrModel.write.overwrite().save(lrModelPath)
    lrModel.transform(preprocessedDf)
  }

  def printMetrics(predictions: DataFrame) = {
    println("Raw predictions")
    predictions.show()
    PredictionMetrics.printMetrics(predictions)
    PredictionMetrics.printConfusionMatrix(predictions)
  }

  /**
    * This method reduces the number of rows in the non hate speech dataframe to match with the
    * number of hate speech dataframe. Since our sample contains imbalanced dataset we are going to
    * undersample our non hate speech data frame.
    *
    * @param df
    * @return
    */
  def underSample(df: DataFrame) = {
    val hateDf = df.filter(df("label") === "1")
    val nonHateDf = df.filter(df("label") === "0")
    val sampleRatio = hateDf.count().toDouble / df.count().toDouble
    val nonHateSampleDf = nonHateDf.sample(false, sampleRatio)
    hateDf.union(nonHateSampleDf)
  }
}
