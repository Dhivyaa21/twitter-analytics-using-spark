package com.me.spark.twitter.preprocessing

import org.apache.spark.sql.DataFrame

trait TextPreprocessor {
  val modelPath: String

  val trainDf: DataFrame

  def getPreprocessedTweetsForDatasetOne() = {
    val pipeline = TextPreprocessingPipeline.createPipeline
    val preprocessedModel = pipeline.fit(trainDf)
    preprocessedModel.write.overwrite().save(modelPath)
    val preprocessedDf = preprocessedModel.transform(trainDf).cache()
    preprocessedDf
  }
}
