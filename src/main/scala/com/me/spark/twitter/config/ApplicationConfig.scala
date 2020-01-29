package com.me.spark.twitter.config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.Logger

/**
  * This class loads all application specific configuration
  */
object ApplicationConfig {
  val logger = Logger.getLogger(getClass.getName)

  var projectDir = s"file:///${System.getProperty("user.dir")}"

  def loadConfig(): Config = {
    logger.info("Loading configuration for the application")
    ConfigFactory.load()
  }

  val applicationConfig: Config = loadConfig()

  val dataSet1 = "ds1"
  val dataSet2 = "ds2"
  val dataSet3 = "sentiment140"
  val trainingDS1 = applicationConfig.getString("config.datasets.one.train")
  val testDS1 = applicationConfig.getString("config.datasets.one.test")
  val DS2 = applicationConfig.getString("config.datasets.two.labelledData")
  val sentiment140Dataset = applicationConfig.getString("config.datasets.three.train")
  val sentiment140DatasetTest = applicationConfig.getString("config.datasets.three.test")
  val preprocessedModelPath = applicationConfig.getString("config.ml.preprocessed.model.path")
  val nbModelPath = applicationConfig.getString("config.ml.naiveBayes.model.path")
  val lrModelPath = applicationConfig.getString("config.ml.logisticRegression.model.path")
  val predictionsPath = applicationConfig.getString("config.datasets.prediction")
  val nbPredictionsPath = predictionsPath + "_" + applicationConfig.getString("config.ml.naiveBayes.prediction.suffix")
  val lrPredictionsPath = predictionsPath + "_" + applicationConfig.getString("config.ml.logisticRegression.prediction.suffix")
  val resultDS1 = applicationConfig.getString("config.datasets.one.result")

}
