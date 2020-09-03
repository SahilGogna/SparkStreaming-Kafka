package com.bigdata.kafka

import java.time.Duration
import java.util.Properties

import com.bigdata.entities.StopTimes
import com.bigdata.spark.SparkEnrichment.spark
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

object KafkaConsumer extends App {

  val consumerProperties = new Properties()
  consumerProperties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  consumerProperties.setProperty(GROUP_ID_CONFIG, "group-id-2")
  consumerProperties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProperties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest")
  consumerProperties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false")

  val consumer = new KafkaConsumer[String, String](consumerProperties)
  consumer.subscribe(List("stop_times_1").asJava)

  println("| Key | Message | Partition | Offset |")

  while (true) {
    val polledRecords: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1).getSeconds)
    var `list`: List[StopTimes] = List()
    if (!polledRecords.isEmpty) {
      println(s"Polled ${polledRecords.count()} records")
      val recordIterator = polledRecords.iterator()
      while (recordIterator.hasNext) {
        val record: ConsumerRecord[String, String] = recordIterator.next()
        val dataRecord = record.value().split(",")
        list = list ++ List(StopTimes(dataRecord(0), dataRecord(1), dataRecord(2), dataRecord(3), dataRecord(4).toInt))
      }

//      val listOfTuples = list.map(record =>
//        Row(record.trip_id,
//          record.stop_id,
//          record.departure_time,
//          record.arrival_time,
//          record.stop_sequence))
//
//      val someSchema = List(
//        StructField("trip_id", StringType, true),
//        StructField("stop_id", StringType, true),
//        StructField("departure_time", StringType, true),
//        StructField("arrival_time", StringType, true),
//        StructField("stop_sequence", StringType, true)
//      )
//      //
//      //      val df = listOfTuples.toDF("trip_id",
//      //        "stop_id",
//      //        "departure_time",
//      //        "arrival_time",
//      //        "stop_sequence")
//
//      val someDF = spark.createDataFrame(
//        spark.sparkContext.parallelize(listOfTuples),
//        StructType(someSchema)
//      )
//
//      someDF.printSchema()
    }
  }

}
