package com.techsophy
package collections

import scala.annotation.tailrec

object part2 extends App {
//  PART 2

  //  1. Implement map using tail recursion - def map(values: List[Int], f: Int => Int): List[Int]
  @tailrec
  def map(values: List[Int], f: Int => Int, result: List[Int] = List.empty[Int]): List[Int] = {
    if (values.isEmpty) result
    else map(
      values.takeRight(values.size - 1),
      f,
      result.appendedAll(List(f(values.head)))
    )
  }

  //  2. Implement flatMap using tail recursion - def map(values: List[Int], f: Int => List[Int]): List[Int]
  @tailrec
  def flatMap(values: List[Int], f: Int => List[Int], result: List[Int] = List.empty[Int]): List[Int] = {
    if (values.isEmpty) result
    else flatMap(values.takeRight(values.size - 1), f, result.appendedAll(f(values.head)))
  }

  //  3. Implement filter using tail recursion - def filter(values: List[Int], f: Int => Boolean): List[Int]]
  def filter(values: List[Int], f: Int => Boolean): List[Int] = ???

  //  4. Implement reduceLeft using tail recursion - def reduceLeft(values: List[Int], f: (Int, Int) => Int): Int
  def reduceLeft(values: List[Int], f: (Int, Int) => Int): Int = ???

  //  5. Implement foldLeft using tail recursion - def foldLeft(values: List[Int], initialValue: Int, f: (Int, Int) => Int): Int
  def foldLeft(values: List[Int], initialValue: Int, f: (Int, Int) => Int): Int = ???


  val numbers = List(1, 2, 3, 4, 5)
  println(map(numbers, _ * 2))
  println(flatMap(numbers, List.fill(2)(_)))
}
