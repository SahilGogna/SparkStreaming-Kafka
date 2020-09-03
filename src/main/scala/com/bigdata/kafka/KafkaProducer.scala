package com.bigdata.kafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object KafkaProducer extends App {

  val topicName = "stop_times_3"

  val producerProperties = new Properties()
  producerProperties.setProperty(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
  )
  producerProperties.setProperty(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
  )
  producerProperties.setProperty(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName
  )

  val producer = new KafkaProducer[String, String](producerProperties)

  Source.fromFile("/Users/sahilgogna/Desktop/gtfs_stm/stop_times.txt")
    .getLines()
    .drop(1)
    .take(5000000)
    .foreach( line => {
      val tripId = line.split(",")(0)
      producer.send(new ProducerRecord[String, String](topicName, tripId, line))
    })

  producer.flush()

}
