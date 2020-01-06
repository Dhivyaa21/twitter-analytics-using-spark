package com.me.spark.twitter.spark.sparkml

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * This object will reduce the number of non hate speech tweets to match with
  * the count of number of hate speech tweets in the dataset.
  * Balancing of such is done by the k-means clustering algorithm.
  */
object KMeansClustering {

  /**
    * balances the dataset to have equal number of hate and non hate speech
    *
    * @param dataFrame    hate speech tweet dataframe
    * @param count        number of rows in the dataframe
    * @param sparkSession [[SparkSession]] object
    * @return [[DataFrame]] object with row count reduced to the count passed in the function
    */
  def balanceDataframe(dataFrame: DataFrame, count: Int)(implicit sparkSession: SparkSession) = {
    val kMeans = new KMeans().setK(count).setMaxIter(30)
    val kMeansModel = kMeans.fit(dataFrame)

    import sparkSession.implicits._
    kMeansModel.clusterCenters.toList.map(v => (v, 0)).toDF("features", "label")
  }
}
