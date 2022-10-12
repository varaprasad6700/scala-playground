package com.techsophy
package sample

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object Schemas {
  val basicDataSchema: StructType = StructType(
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

  val crewDataSchema: StructType = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("directors", StringType, nullable = true),
      StructField("writers", StringType, nullable = true),
    )
  )

  val crewNamesDataSchema: StructType = StructType(
    List(
      StructField("nconst", StringType, nullable = false),
      StructField("primaryName", StringType, nullable = true),
      StructField("birthYear", IntegerType, nullable = true),
      StructField("deathYear", IntegerType, nullable = true),
      StructField("primaryProfession", StringType, nullable = true),
      StructField("knownForTitles", StringType, nullable = true),
    )
  )

  val episodeDataSchema: StructType = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("parentTconst", StringType, nullable = false),
      StructField("seasonNumber", IntegerType, nullable = true),
      StructField("episodeNumber", IntegerType, nullable = false),
    )
  )

  val ratingsDataSchema: StructType = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("averageRating", DoubleType, nullable = true),
      StructField("numVotes", LongType, nullable = true),
    )
  )

  val akasDataSchema: StructType = StructType(
    List(
      StructField("titleId", StringType, nullable = false),
      StructField("ordering", IntegerType, nullable = true),
      StructField("title", StringType, nullable = true),
      StructField("region", StringType, nullable = false),
      StructField("language", StringType, nullable = true),
      StructField("types", StringType, nullable = true),
      StructField("attributes", StringType, nullable = true),
      StructField("isOriginalTitle", IntegerType, nullable = true),
    )
  )
}
