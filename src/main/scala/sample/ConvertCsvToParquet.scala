package com.techsophy
package sample

import sample.Schemas._

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ConvertCsvToParquet extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  val basicData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(basicDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.basics.tsv/data.tsv")


  val crewData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(crewDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.crew.tsv/data.tsv")

  val crewNamesData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(crewNamesDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/name.basics.tsv/data.tsv")

  val episodeData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(episodeDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.episode.tsv/data.tsv")

  val ratingsData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(ratingsDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.ratings.tsv/data.tsv")

  val akasData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(akasDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.akas.tsv/data.tsv")


  crewData.show

  val limit = 1000
  basicData.write.mode(SaveMode.Overwrite).save("/home/prasad/Downloads/imdb dataset/parquet/basic")
  crewData.write.mode(SaveMode.Overwrite).save("/home/prasad/Downloads/imdb dataset/parquet/crew")
  crewNamesData.write.mode(SaveMode.Overwrite).save("/home/prasad/Downloads/imdb dataset/parquet/name")
  episodeData.write.mode(SaveMode.Overwrite).save("/home/prasad/Downloads/imdb dataset/parquet/episode")
  ratingsData.write.mode(SaveMode.Overwrite).save("/home/prasad/Downloads/imdb dataset/parquet/rating")
  akasData.write.mode(SaveMode.Overwrite).save("/home/prasad/Downloads/imdb dataset/parquet/akas")

}
