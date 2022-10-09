package com.techsophy
package sample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object SparkTaskThree extends App {
  val limit = 100000

  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  val basicDataSchema = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("titleType", StringType, nullable = true),
      StructField("primaryTitle", StringType, nullable = true),
      StructField("originalTitle", StringType, nullable = false),
      StructField("isAdult", IntegerType, nullable = true),
      StructField("startYear", IntegerType, nullable = true),
      StructField("endYear", IntegerType, nullable = true),
      StructField("runTimeMinutes", LongType, nullable = true),
      StructField("genres", StringType, nullable = true),
    )
  )

  val basicData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(basicDataSchema)
    .csv("D:\\imdb datasets\\title.basics.tsv\\data.tsv")
  //    .limit(limit)

  val outputFrame: DataFrame = basicData
    .groupBy(col("startYear")).agg(avg(col("runtimeMinutes")).as("avgRuntime"))
    .sort(col("avgRuntime").desc_nulls_last)

  outputFrame.show

  Thread.sleep(60 * 60 * 1000)
}
