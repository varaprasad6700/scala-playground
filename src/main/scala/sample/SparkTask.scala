package com.techsophy
package sample

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTask extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Task")
    .getOrCreate()

  val basicData: DataFrame = spark.read
    .option("delimiter", "\t")
    .option("header", value = true)
    .csv("/home/prasad/Downloads/imdb dataset/name.basics.tsv/data.tsv")

  basicData.show()
}
