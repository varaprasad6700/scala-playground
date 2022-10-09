package com.techsophy
package sample

import org.apache.spark.sql.functions.{broadcast, col, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTaskTwo extends App {
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

  val episodeDataSchema = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("parentTconst", StringType, nullable = false),
      StructField("seasonNumber", IntegerType, nullable = true),
      StructField("episodeNumber", IntegerType, nullable = false),
    )
  )

  val ratingsDataSchema = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("averageRating", DoubleType, nullable = true),
      StructField("numVotes", LongType, nullable = true),
    )
  )

  val basicData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(basicDataSchema)
    .csv("D:\\imdb datasets\\title.basics.tsv\\data.tsv")

  val ratingsData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(ratingsDataSchema)
    .csv("D:\\imdb datasets\\title.ratings.tsv\\data.tsv")

  val episodeData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(episodeDataSchema)
    .csv("D:\\imdb datasets\\title.episode.tsv\\data.tsv")

  val episodeGroupedDf: DataFrame = episodeData
    .groupBy(col("parentTconst")).count()

  val joinedFrame: DataFrame = basicData
    .filter(col("titleType").equalTo(lit("tvSeries")))
    .join(broadcast(ratingsData), basicData("tconst") === ratingsData("tconst"))
    .join(broadcast(episodeGroupedDf), basicData("tconst") === episodeGroupedDf("parentTconst"))
    .select(basicData("tconst"), col("titleType"), col("primaryTitle"), col("count").as("numberOfEpisodes"), col("averageRating"), col("numVotes"))

  joinedFrame.show()

  Thread.sleep(60 * 60 * 1000)
}
