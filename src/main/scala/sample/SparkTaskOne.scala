package com.techsophy
package sample

import sample.Schemas._

import org.apache.spark.sql.functions.{array_contains, col, explode, lit, split}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTaskOne extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  val basicData: DataFrame = spark.read
    .schema(basicDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet/basic")

  val akasData: DataFrame = spark.read
    .schema(akasDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet/akas")

  val crewData: DataFrame = spark.read
    .schema(crewDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet/crew")

  val crewNamesData: DataFrame = spark.read
    .schema(crewNamesDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet/name")

  val modifiedCrewData = crewData
    .withColumn("directors", split(col("directors"), ","))
    .withColumn("directors", explode(col("directors")))
  //    .withColumn("writers", split(col("writers"), ","))

  //tt3768572
  val joinedFrame: DataFrame = basicData
    .filter((col("titleType") === lit("tvSeries")) &&
      (col("startYear") >= 2010 && col("endYear") <= 2020)
    )
    .join(akasData, basicData.col("tconst") === akasData.col("titleId"), "left_outer")
    .join(modifiedCrewData, basicData.col("tconst") === crewData.col("tconst"), "left_outer")
    .join(crewNamesData, modifiedCrewData.col("directors") === crewNamesData.col("nconst"))
    //    .filter(basicData("tconst") === lit("tt3768572"))
    .select(col("title"), col("genres"), col("primaryName").as("directorName"))

  joinedFrame.show
  Thread.sleep(60 * 60 * 1000)
}
