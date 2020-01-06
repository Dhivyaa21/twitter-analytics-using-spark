package com.me.spark.twitter.spark.config

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

  val trainingDS = applicationConfig.getString("config.datasets.train")
  val testDS = applicationConfig.getString("config.datasets.test")
  val preprocessedModelPath = applicationConfig.getString("config.ml.preprocessed.model.path")
  val nbModelPath = applicationConfig.getString("config.ml.naiveBayes.model.path")
  val lrModelPath = applicationConfig.getString("config.ml.logisticRegression.model.path")
  val predictionsPath = applicationConfig.getString("config.datasets.prediction")
  val nbPredictionsPath = predictionsPath + "_" + applicationConfig.getString("config.ml.naiveBayes.prediction.suffix")
  val lrPredictionsPath = predictionsPath + "_" + applicationConfig.getString("config.ml.logisticRegression.prediction.suffix")
  val resultDS = applicationConfig.getString("config.datasets.result")

}
