package com.techsophy
package sample

import sample.Schemas.basicDataSchema

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTaskThree extends App {
  val limit = 100000

  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  val basicData: DataFrame = spark.read
    .schema(basicDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/basic")
  //    .limit(limit)

  val outputFrame: DataFrame = basicData
    .groupBy(col("startYear")).agg(avg(col("runtimeMinutes")).as("avgRuntime"))
    .sort(col("avgRuntime").desc_nulls_last)

  outputFrame.show

  Thread.sleep(60 * 60 * 1000)
}
