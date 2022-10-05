package com.techsophy
package spark

import org.apache.spark.sql.SparkSession

object SparkReadFile extends App {

  val spark: SparkSession = SparkSession.builder().master("local[6]").appName("readCsv").getOrCreate()

  val df = spark.read
    .format("csv")
    .option("sep", "|")
    .load("C:\\Users\\pothu\\IdeaProjects\\playground\\src\\main\\resources\\data.csv")

  df.describe().show()


  Thread.sleep(60 * 60 * 1000)
}
