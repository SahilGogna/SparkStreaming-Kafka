package com.bigdata.spark

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkStreaming extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Spark streaming with Kafka")

  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(10))

  val kafkaConfig = Map[String, String](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "test",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
  )

  val topic = "stop_times_1"
  val enrichedTripDf = SparkEnrichment.getEnrichedTripDF
  val inStream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](List(topic), kafkaConfig)
  )

  inStream.map(_.value()).foreachRDD(microBatchRdd => businessLogic(microBatchRdd))

  // 4. Start streaming and keep running
  ssc.start()
  ssc.awaitTermination()

  def businessLogic(rdd: RDD[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val record = rdd.map(_.split(","))
      .map(x => (x(0), x(1), x(2), x(3), x(4)))

    val stopTimesDf = record.toDF("trip_id",
      "stop_id",
      "departure_time",
      "arrival_time",
      "stop_sequence")

    val stopEnrichedTripJoinCondition = stopTimesDf.col("trip_id") === enrichedTripDf.col("trip_id")

    val finalEnrichedTrip = stopTimesDf
      .join(enrichedTripDf, stopEnrichedTripJoinCondition)
      .select(enrichedTripDf("trip_id"),
        enrichedTripDf("route_id"),
        enrichedTripDf("service_id"),
        enrichedTripDf("trip_headsign"),
        enrichedTripDf("direction_id"),
        enrichedTripDf("shape_id"),
        enrichedTripDf("wheelchair_accessible"),
        enrichedTripDf("note_fr"),
        enrichedTripDf("note_en"),
        enrichedTripDf("start_time"),
        enrichedTripDf("end_time"),
        enrichedTripDf("headway_secs"),
        enrichedTripDf("date"),
        enrichedTripDf("exception_type"),
        stopTimesDf("stop_id"),
        stopTimesDf("departure_time"),
        stopTimesDf("arrival_time"),
        stopTimesDf("stop_sequence"))

    System.out.println("Count of enriched trips " + enrichedTripDf.count())
    finalEnrichedTrip.printSchema()


    val path = s"src/main/resources/" + System.nanoTime()

    finalEnrichedTrip
        .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .save(path)
  }

}
