package com.techsophy
package sample

import sample.Schemas._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTaskFour extends App {
  val limit = 10000

  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  val basicData: DataFrame = spark.read
    .schema(basicDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/basic")

  val crewData: DataFrame = spark.read
    .schema(crewDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/crew")

  val crewNamesData: DataFrame = spark.read
    .schema(crewNamesDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/name")


  val modifiedCrewData = crewData
    .withColumn("directors", split(col("directors"), ","))
  //    .withColumn("writers", split(col("writers"), ","))

  val modifiedBasicData = basicData
    .withColumn("genres", split(col("genres"), ","))
    .withColumn("genres", explode(col("genres")))
  //    .select("tconst", "genres")

  val crewName: DataFrame = modifiedCrewData
    .join(crewNamesData, array_contains(modifiedCrewData.col("directors"), crewNamesData.col("nconst")))
  //      .select("tconst", "primaryName")

  crewName.show(1000)

  val joinedFrame: DataFrame = modifiedBasicData
    .join(crewName, modifiedBasicData("tconst") === crewData.col("tconst"))
    .groupBy(modifiedBasicData("genres")).agg(collect_set(col("primaryName")).as("directorNames"))

  joinedFrame.show(1000)

  Thread.sleep(60 * 60 * 1000)
}
