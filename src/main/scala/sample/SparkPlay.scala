package com.techsophy
package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkPlay extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[3]")
    .appName("Basics")
    .getOrCreate()

  // RDD - Resilient distributed dataset - Seq[Row]
  // DataFrame
  // Dataset

  import spark.implicits._

  val employees: DataFrame = Seq(
    (1, "Aman", "Developer"),
    (2, "Amit", "Sr.Developer"),
    (3, "Rohan", "Tester"),
    (4, "Rohit", "Sr.Developer"),
  ).toDF("empId", "empName", "designation")

  //employees.show()  // truncated representation
  //employees.count()
  /**
   * Every ACTION will create a SPARK JOB
   * Every JOB is divided into STAGES -> divided into TASKS (actually executed by the executors)
   * - Whenever there is a shuffle, a new stage will be created.
   * - The preceeding stage must be completed before moving ahead.
   * ** Avoid shuffling, if possible.
   */
  // employees.withColumn("empName", concat(lit("Mr. "), col("empName"))).show()
  //employees.withColumn("id", col("empId") + 100).show()
  // employees.withColumn("companyName", lit("Techsophy")).show()
  // employees.show(2, false)  // 2 rows and non-truncated rep
  // employees.withColumnRenamed("empName", "name")
  // employees.drop("designation").show()
  // employees.select(col("empId").as("id")).show()
  // val cond = col("designation").contains("Developer")

  import org.apache.spark.sql.functions._

  val value: Dataset[Row] = employees
    .withColumn("name", concat(lit("Mr. "), col("empName")))
    .filter(col("designation").contains("Developer"))
    .distinct()
    .filter(col("designation").contains("Sr."))

  value
    .show()

  case class Employee(empId: Int, empName: String, designation: String)

  case class Designation(designation: String, processor: String)


  val seq = Seq(
    Employee(1, "Aman", "Developer"),
    Employee(2, "Amit", "Sr.Developer"),
    Employee(3, "Rohan", "Tester"),
    Employee(4, "Rohit", "Sr.Developer"),
  )

  val seq1 = Seq(
    Employee(5, "prasad", "Asst. Developer"),
    Employee(2, "Amit", "Sr.Developer"),
  )

  val designationSeq = Seq(
    Designation("Developer", "5700x"),
    Designation("Sr.Developer", "5950x"),
    Designation("Analyst", "5600x")
  )

  val employees1: DataFrame = seq.toDF("empId", "empName", "designation")
  val employees2: DataFrame = seq1.toDF("empId", "empName", "designation")
  val designation1: DataFrame = designationSeq.toDF("designation", "processor")

  employees1.union(employees2).show()

  employees1.union(employees2).distinct().show()

  List("inner", "cross", "outer", "full", "fullouter", "full_outer", "left", "leftouter", "left_outer", "right", "rightouter", "right_outer", "semi", "leftsemi", "left_semi", "anti", "leftanti", "left_anti")
    .foreach(joinType => {
      employees1.join(designation1, employees1("designation") === designation1("designation"), joinType).show()
      println(joinType)
    })
  //  employees1.join(designation1, col("designation") === col("designation"))


  // RDD - Seq[Row]
  // Dataset - T (Serializable - case class)
  // DataFrame = Dataset[T]
  val rdd: RDD[Employee] = spark.sparkContext.parallelize(seq)
    .map(row => Employee(row.empId + 100, row.empName, row.designation))

  rdd.toDF()

  val ids = employees.collect().map(row => row.getAs[Int](0)).toList

  val empDS: Dataset[Employee] = employees.as[Employee]

  //empDS.map(emp => emp.empId).show()
  //println(ids)
  employees.describe().show()

  Thread.sleep(100 * 60 * 1000)

  // UNION
  //  val df1: DataFrame = ???  // cols - id, name, address
  //  val df2: DataFrame = ???  // cols - name, id, address
  //  val unionDF: DataFrame = df1.union(df2)
  //
  //  // JOIN
  //  val df3: DataFrame = ???  // cols - id, name, address
  //  val df4: DataFrame = ???  // cols - empId, salary
  //  df3.filter(col("id") === 5).show()
  //
  //  /**
  //   * inner
  //   * left_outer
  //   * right_outer
  //   * full_outer / outer
  //   */
  //  val joinDF: DataFrame = df3.join(df4, Seq("id"), "left_outer")
  //
  //  val joinCond = df3("id").isNotNull && df3("id") === df4("empId")
  //
  //  val colJoinDF = df3.join(df4, joinCond)
  //
  //  // GROUP BY - AGGREGATIONS
  //  val df5: DataFrame = ???  // cols - studentId, subject, marks
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
  //employees.printSchema() // print the schema
  Thread.sleep(100 * 60 * 1000)
}
