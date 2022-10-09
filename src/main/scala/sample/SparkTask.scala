package com.techsophy
package sample

import org.apache.spark.sql.functions.{array_contains, col, split}
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTask extends App {
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

  val akasDataSchema = StructType(
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

  val crewDataSchema = StructType(
    List(
      StructField("tconst", StringType, nullable = false),
      StructField("directors", StringType, nullable = true),
      StructField("writers", StringType, nullable = true),
    )
  )

  val basicData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(basicDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.basics.tsv/data.tsv")

  val akasData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(akasDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.akas.tsv/data.tsv")

  val crewData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = false)
    .option("nullValue", "\\N")
    .schema(crewDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.crew.tsv/data.tsv")

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
    .csv("/home/prasad/Downloads/imdb dataset/name.basics.tsv/data.tsv")

  val modifiedCrewData = crewData
    .withColumn("directors", split(col("directors"), ","))
    .withColumn("writers", split(col("writers"), ","))

  //  basicData.filter(col("titleId").isNull).show
  basicData.printSchema

  println(basicData.count())

  //  akasData.show()
  akasData.printSchema

  modifiedCrewData.printSchema

  val joinedFrame: DataFrame = basicData.filter(col("startYear").geq(2010).and(col("endYear").leq(2020)))
    .join(akasData, basicData.col("tconst") === akasData.col("titleId"), "left_outer")
    .join(modifiedCrewData, basicData.col("tconst") === crewData.col("tconst"), "left_outer")
    .join(crewNamesData, array_contains(modifiedCrewData.col("directors"), crewNamesData.col("nconst")))
    .select(col("title"), col("genres"), col("primaryName").as("directoryName"))

  joinedFrame.write.csv("a.csv")
  joinedFrame.show()
  Thread.sleep(60 * 60 * 1000)
}
