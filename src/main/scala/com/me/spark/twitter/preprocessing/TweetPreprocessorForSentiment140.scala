package com.me.spark.twitter.preprocessing

import com.me.spark.twitter.config.{ApplicationConfig, SparkConfig}
import com.me.spark.twitter.jobs.ImportFromCSV
import com.me.spark.twitter.preprocessing.traits.TextPreprocessor
import org.apache.spark.sql.DataFrame

object TweetPreprocessorForSentiment140 extends SparkConfig with TextPreprocessor {
  override val modelPath: String = ApplicationConfig.preprocessedModelPath + '/' + ApplicationConfig.dataSet3
  override val trainDf: DataFrame = ImportFromCSV.readSentiment140Csv(ApplicationConfig.sentiment140Dataset, ImportFromCSV.sentiment140Schema)
    .repartition(15).persist()
}
