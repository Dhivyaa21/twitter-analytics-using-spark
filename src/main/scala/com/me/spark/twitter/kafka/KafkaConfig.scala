package com.me.spark.twitter.kafka

import com.me.spark.twitter.config.ApplicationConfig
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaConfig {

  def publishToKafka(df: DataFrame)(implicit sparkSession: SparkSession): Unit = {
    println("publishing to kafka")
    df.selectExpr("CAST(tweetId AS STRING) AS key", "to_json(struct(tweetId, prediction)) AS value")
      .write
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", ApplicationConfig.kafkaBootstrapServer)
      .option("topic", ApplicationConfig.kafkaTopic)
      .option("key.serializer", ApplicationConfig.kafkaStringSerializer)
      .option("value.serializer", ApplicationConfig.kafkaStringSerializer)
      .save()
  }

  def subscribeToKafka()(implicit sparkSession: SparkSession): Unit = {
    println("Reading from kafka")
    val df = sparkSession
      .read
      .format("org.apache.spark.sql.kafka010.KafkaSourceProvider")
      .option("kafka.bootstrap.servers", ApplicationConfig.kafkaBootstrapServer)
      .option("subscribe", ApplicationConfig.kafkaTopic)
      .option("key.deserializer", ApplicationConfig.kafkaStringDeserializer)
      .option("value.deserializer", ApplicationConfig.kafkaStringDeserializer)
      .option("startingOffsets", "earliest")
      .load()

    val schema = StructType(
      Seq(StructField("tweetId", LongType, false),
        StructField("prediction", DoubleType, false))
    )

    import sparkSession.implicits._
    val deserializedDf = df.selectExpr("CAST(value as STRING) as json")
      .select(from_json($"json", schema) as "data")
      .select("data.*")

    deserializedDf.show()
  }
}
