package com.techsophy
package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkPlay extends App {

  case class Employee(empId: Int, empName: String, designation: String)

  val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("weee")
    .getOrCreate()

  val seq = Seq(
    Employee(1, "Aman", "Developer"),
    Employee(2, "Amit", "Sr.Developer"),
    Employee(3, "Rohan", "Tester"),
    Employee(4, "Rohit", "Sr.Developer"),
  )

  val seq2 = Seq(
    Employee(5, "prasad", "Asst.Developer"),
    Employee(1, "Aman", "Developer")
  )

  import spark.implicits._


  val employees: DataFrame = seq.toDF("empId", "empName", "designation")

  val employees2: DataFrame = seq.toDF("empId", "empName", "designation")

  employees.union(employees2).distinct().show()
  //  //   RDD - Seq[Row]
  //  //   Dataset - T (Serializable - case class)
  //  //   DataFrame = Dataset[T]
  //  val rdd: RDD[Employee] = spark.sparkContext.parallelize(seq)
  //    .map(row => Employee(row.empId + 100, row.empName, row.designation))
  //  val ids: List[Int] = employees.collect().map(row => row.getAs[Int](0)).toList

  //  rdd.toDF()
  //  val empDS: Dataset[Employee] = employees.as[Employee]
  //  // UNION
  //  val df1: DataFrame = ??? // cols - id, name, address
  //
  //  //empDS.map(emp => emp.empId).show()
  //  //println(ids)
  //  employees.describe().show()
  //
  //  Thread.sleep(100 * 60 * 1000)
  //  val df2: DataFrame = ??? // cols - name, id, address
  //  val unionDF: DataFrame = df1.union(df2)
  //  // JOIN
  //  val df3: DataFrame = ??? // cols - id, name, address
  //  val df4: DataFrame = ??? // cols - empId, salary
  //  /**
  //   * inner
  //   * left_outer
  //   * right_outer
  //   * full_outer / outer
  //   */
  //  val joinDF: DataFrame = df3.join(df4, Seq("id"), "left_outer")
  //  df3.filter(col("id") === 5).show()
  //  val joinCond = df3("id").isNotNull && df3("id") === df4("empId")
  //  val colJoinDF = df3.join(df4, joinCond)
  //  // GROUP BY - AGGREGATIONS
  //  val df5: DataFrame = ??? // cols - studentId, subject, marks
  //
  //
  //  df5
  //    .groupBy("studentId", "subject")
  //    .agg(
  //      sum("marks").as("total_marks"),
  //      avg("marks").as("avg_marks"),
  //      collect_list("marks").as("marks")
  //    )
  //    .show()
  //
  //  // HANDLING NULL
  //  df5.na.drop()
  //  df5.na.fill(Map("marks" -> 0))
  Thread.sleep(10 * 60 * 1000)
}
