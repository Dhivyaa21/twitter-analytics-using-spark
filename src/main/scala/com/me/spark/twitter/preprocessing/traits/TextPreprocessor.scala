package com.me.spark.twitter.preprocessing.traits

import com.me.spark.twitter.preprocessing.TextPreprocessingPipeline
import org.apache.spark.sql.DataFrame

trait TextPreprocessor {
  val modelPath: String

  val trainDf: DataFrame

  def getPreprocessedTweets() = {
    val pipeline = TextPreprocessingPipeline.createPipeline
    val preprocessedModel = pipeline.fit(trainDf)
    preprocessedModel.write.overwrite().save(modelPath)
    val preprocessedDf = preprocessedModel.transform(trainDf).cache()
    preprocessedDf
  }
}
