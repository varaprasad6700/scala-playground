package com.techsophy
package sample

import org.apache.spark.sql.functions.{array_contains, col, collect_set, explode, split}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTaskFour extends App {
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
    .limit(limit)

  val crewDataSchema = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("directors", StringType, nullable = true),
      StructField("writers", StringType, nullable = true),
    )
  )

  val crewData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(crewDataSchema)
    .csv("D:\\imdb datasets\\title.crew.tsv\\data.tsv")
    .limit(limit)


  val crewNamesDataSchema = StructType(
    List(
      StructField("nconst", StringType, nullable = false),
      StructField("primaryName", StringType, nullable = true),
      StructField("birthYear", IntegerType, nullable = true),
      StructField("deathYear", IntegerType, nullable = true),
      StructField("primaryProfession", StringType, nullable = true),
      StructField("knownForTitles", StringType, nullable = true),
    )
  )

  val crewNamesData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(crewNamesDataSchema)
    .csv("D:\\imdb datasets\\name.basics.tsv\\data.tsv")
    .limit(limit)


  val modifiedCrewData = crewData
    .withColumn("directors", split(col("directors"), ","))
    .withColumn("writers", split(col("writers"), ","))

  val modifiedBasicData = basicData
    .withColumn("genres", split(col("genres"), ","))
    .withColumn("genres", explode(col("genres")))
    .select("tconst", "genres")

  val crewName: DataFrame = modifiedCrewData
    .select("tconst", "directors")
    .join(crewNamesData, array_contains(modifiedCrewData.col("directors"), crewNamesData.col("nconst")))
    .select("tconst", "primaryName")

  crewName.show()

  val joinedFrame: DataFrame = modifiedBasicData
    .join(crewName, modifiedBasicData("tconst") === crewData.col("tconst"))
    .coalesce(8)
    .groupBy(modifiedBasicData("genres")).agg(collect_set(col("primaryName")).as("directorNames"))


  joinedFrame.show

  Thread.sleep(60 * 60 * 1000)
}
