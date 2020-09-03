package com.bigdata.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._


object SparkEnrichment{

  def getEnrichedTripDF: DataFrame = {

    val spark = SparkSession.builder()
      .appName("Spark_Kafka")
      .config("spark.master", "local")
      .getOrCreate()

    // reading a DF
    //  val tripsDF = spark.read.textFile("/user/fall2019/sahilgogna/project5/trips/trips.txt")

    val tripsSchema = StructType(Array(
      StructField("route_id", IntegerType),
      StructField("service_id", StringType),
      StructField("trip_id", StringType),
      StructField("trip_headsign", StringType),
      StructField("direction_id", IntegerType),
      StructField("shape_id", IntegerType),
      StructField("wheelchair_accessible", BooleanType),
      StructField("note_fr", StringType),
      StructField("note_en", StringType)
    ))

    val tripsDF = spark.read
      //      .schema(tripsSchema)
      .option("inferschema", "true")
      .format("CSV")
      .option("header", "true")
      .load("/Users/sahilgogna/Desktop/gtfs_stm/trips.txt")

    val frequenciesDF = spark.read
      .option("inferschema", "true")
      .format("CSV")
      .option("header", "true")
      .load("/Users/sahilgogna/Desktop/gtfs_stm/frequencies.txt")

    val calendarDatesDF = spark.read
      .option("inferschema", "true")
      .format("CSV")
      .option("header", "true")
      .load("/Users/sahilgogna/Desktop/gtfs_stm/calendar_dates.txt")

    import spark.implicits._

    val tripFreqJoinCondition = tripsDF.col("trip_id") === frequenciesDF.col("trip_id")

    val tripFrequencyDF = tripsDF
      .join(frequenciesDF, tripFreqJoinCondition, "left")
      .select(tripsDF("trip_id"),
        tripsDF("route_id"),
        tripsDF("service_id"),
        tripsDF("trip_headsign"),
        tripsDF("direction_id"),
        tripsDF("shape_id"),
        tripsDF("wheelchair_accessible"),
        tripsDF("note_fr"),
        tripsDF("note_en"),
        frequenciesDF("start_time"),
        frequenciesDF("end_time"),
        frequenciesDF("headway_secs"))

    val tripFreqCalJoinCondition = tripFrequencyDF.col("service_id") === calendarDatesDF.col("service_id")

    val enrichedTripDF = tripFrequencyDF
      .join(calendarDatesDF, tripFreqCalJoinCondition, "left")
      .select(tripFrequencyDF("trip_id"),
        tripFrequencyDF("route_id"),
        tripFrequencyDF("service_id"),
        tripFrequencyDF("trip_headsign"),
        tripFrequencyDF("direction_id"),
        tripFrequencyDF("shape_id"),
        tripFrequencyDF("wheelchair_accessible"),
        tripFrequencyDF("note_fr"),
        tripFrequencyDF("note_en"),
        tripFrequencyDF("start_time"),
        tripFrequencyDF("end_time"),
        tripFrequencyDF("headway_secs"),
        calendarDatesDF("date"),
        calendarDatesDF("exception_type"))

    enrichedTripDF
  }

}
