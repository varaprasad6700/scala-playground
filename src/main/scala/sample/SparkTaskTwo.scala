package com.techsophy
package sample

import sample.Schemas._

import org.apache.spark.sql.functions.{broadcast, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTaskTwo extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  val basicData: DataFrame = spark.read
    .schema(basicDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/basic")

  val ratingsData: DataFrame = spark.read
    .schema(ratingsDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/rating")

  val episodeData: DataFrame = spark.read
    .schema(episodeDataSchema)
    .load("/home/prasad/Downloads/imdb dataset/parquet-limited/episode")

  val episodeGroupedDf: DataFrame = episodeData
    .groupBy(col("parentTconst")).count()

  val joinedFrame: DataFrame = basicData
    .filter(col("titleType").equalTo(lit("tvSeries")))
    .join(broadcast(ratingsData), basicData("tconst") === ratingsData("tconst"))
    .join(broadcast(episodeGroupedDf), basicData("tconst") === episodeGroupedDf("parentTconst"))
    .select(basicData("tconst"), col("titleType"), col("primaryTitle"), col("count").as("numberOfEpisodes"), col("averageRating"), col("numVotes"))

  joinedFrame.show(1000)

  Thread.sleep(60 * 60 * 1000)
}
