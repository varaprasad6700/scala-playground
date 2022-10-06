package com.techsophy
package sample

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTask extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("Task")
    .getOrCreate()

  //  val basicDataSchema = StructType(
  //    List(
  //      StructField("tconst", StringType, nullable = false),
  //      StructField("titleType", StringType, nullable = true),
  //      StructField("primaryTitle", StringType, nullable = true),
  //      StructField("originalTitle", StringType, nullable = false),
  //      StructField("isAdult", IntegerType, nullable = true),
  //      StructField("startYear", IntegerType, nullable = true),
  //      StructField("endYear", IntegerType, nullable = true),
  //      StructField("runTimeMinutes", LongType, nullable = true),
  //      StructField("genres", ArrayType(StringType), nullable = true),
  //    )
  //  )

  val basicData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .option("nullValue", "\\N")
    //    .schema(basicDataSchema)
    .csv("/home/prasad/Downloads/imdb dataset/title.basics.tsv/data.tsv")

  val akasData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .option("nullValue", "\\N")
    .csv("/home/prasad/Downloads/imdb dataset/title.akas.tsv/data.tsv")

  val crewData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .option("inferSchema", value = true)
    .option("nullValue", "\\N")
    .csv("/home/prasad/Downloads/imdb dataset/title.crew.tsv/data.tsv")

  //  basicData.filter(col("titleId").isNull).show
  basicData.printSchema

  //  akasData.show()
  akasData.printSchema

  crewData.printSchema

  basicData.filter(col("startYear").geq(2010).and(col("endYear").leq(2020)))
    .join(akasData, basicData.col("tconst") === akasData.col("titleId"), "left_outer")
    .join(crewData, basicData.col("tconst") === crewData.col("tconst"), "left_outer")
    .show(100)

  Thread.sleep(60 * 60 * 1000)
}
