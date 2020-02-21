package com.me.spark.twitter.kafka

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object KafkaSource {

  val topic = "predictions"

  def main(args: Array[String]): Unit = {
    produceMessages(topic)
    consumeMessages(topic)
  }

  def produceMessages(topic: String): Unit = {
    val producerProperties = new Properties()
    producerProperties.put("bootstrap.servers", "localhost:9092")
    producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](producerProperties)
    val record = new ProducerRecord[String, String](topic, "key", "value")
    try {
      producer.send(record)
    } catch {
      case e: Exception => System.err.println(e.getMessage)
    } finally {
      producer.close()
    }
  }

  def consumeMessages(topic: String): Unit = {
    val consumerProperties = new Properties()
    consumerProperties.put("bootstrap.servers", "localhost:9092")
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProperties.put("auto.offset.reset", "latest")
    consumerProperties.put("group.id", "consumer-group")
    val consumer = new KafkaConsumer[String, String](consumerProperties)
    consumer.subscribe(util.Arrays.asList(topic))
    try {
      while (true) {
        val record = consumer.poll(Duration.ofSeconds(10))
        val it = record.iterator()
        while (it.hasNext) {
          val data = it.next()
          println(data.value())
        }
      }
    } catch {
      case e: Exception => System.err.print(e.getMessage)
    } finally {
      consumer.close()
    }
  }
}
