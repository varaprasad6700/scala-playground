package com.techsophy
package collections

import scala.annotation.tailrec

object Part2 extends App {
  //  PART 2

  val numbers = List(1, 2, 3, 4, 5)

  //  1. Implement map using tail recursion - def map(values: List[Int], f: Int => Int): List[Int]
  @tailrec
  def map(values: List[Int], f: Int => Int, result: List[Int] = List.empty[Int]): List[Int] = {
    if (values.isEmpty) result
    else map(
      values.tail, // can use indexing or values.takeRight(size-1)
      f,
      result.appendedAll(List(f(values.head)))
    )
  }

  def map_v2(values: List[Int], f: Int => Int): List[Int] = {
    @tailrec
    def loop(rem: List[Int], acc: List[Int] = List.empty[Int]): List[Int] = {
      rem.headOption match {
        case None => acc
        case Some(head) => loop(rem.tail, f(head) :: acc)
      }
    }

    loop(values).reverse
  }

  //  2. Implement flatMap using tail recursion - def map(values: List[Int], f: Int => List[Int]): List[Int]
  @tailrec
  def flatMap(values: List[Int], f: Int => List[Int], result: List[Int] = List.empty[Int]): List[Int] = {
    if (values.isEmpty) result
    else flatMap(
      values.tail,
      f,
      result.appendedAll(f(values.head))
    )
  }

  def flatMap_v2(values: List[Int], f: Int => List[Int]): List[Int] = {
    @tailrec
    def loop(rem: List[Int], acc: List[Int] = List.empty[Int]): List[Int] = {
      rem.headOption match {
        case None => acc
        case Some(head) => loop(rem.tail, f(head) ::: acc)
      }
    }

    loop(values).reverse
  }

  //  3. Implement filter using tail recursion - def filter(values: List[Int], f: Int => Boolean): List[Int]]
  @tailrec
  def filter(values: List[Int], f: Int => Boolean, result: List[Int] = List.empty[Int]): List[Int] = {
    if (values.isEmpty) result
    else filter(
      values.tail,
      f,
      if (f(values.head)) result.appendedAll(List(values.head)) else result
    )
  }

  def filter_v2(values: List[Int], f: Int => Boolean): List[Int] = {
    @tailrec
    def loop(rem: List[Int], acc: List[Int] = List.empty[Int]): List[Int] = {
      rem.headOption match {
        case None => acc
        case Some(head) if f(head) => loop(rem.tail, head :: acc)
        case _ => loop(rem.tail, acc)
      }
    }

    loop(values).reverse
  }

  //  4. Implement reduceLeft using tail recursion - def reduceLeft(values: List[Int], f: (Int, Int) => Int): Int
  @tailrec
  def reduceLeft(values: List[Int], f: (Int, Int) => Int, acc: Option[Int] = None): Int = {
    if (acc.isEmpty && values.isEmpty) throw new UnsupportedOperationException
    else if (values.isEmpty) acc.get
    else reduceLeft(
      values.tail,
      f,
      if (acc.isEmpty) Option(values.head) else Option(f(acc.get, values.head))
    )
  }

  def reduceLeft_v2(values: List[Int], f: (Int, Int) => Int): Option[Int] = {
    @tailrec
    def loop(rem: List[Int], acc: Option[Int] = None): Option[Int] = {
      (rem.headOption, acc) match {
        case (Some(head), Some(reduced)) => loop(rem.tail, Some(f(head, reduced)))
        case (Some(head), None) => loop(rem.tail, Some(head))
        case _ => acc
      }
    }

    loop(values, None)
  }

  //  5. Implement foldLeft using tail recursion - def foldLeft(values: List[Int], initialValue: Int, f: (Int, Int) => Int): Int
  @tailrec
  def foldLeft(values: List[Int], initialValue: Int, f: (Int, Int) => Int, acc: Option[Int] = None): Int = {
    if (values.isEmpty) acc.getOrElse(initialValue)
    else foldLeft(
      values.tail,
      initialValue,
      f,
      Option(f(acc.getOrElse(initialValue), values.head))
    )
  }

  def foldLeft_v2(values: List[Int], initialValue: Int, f: (Int, Int) => Int): Option[Int] = {
    @tailrec
    def loop(rem: List[Int], acc: Option[Int] = None): Option[Int] = {
      (rem.headOption, acc) match {
        case (Some(head), Some(reduced)) => loop(rem.tail, Some(f(head, reduced)))
        case (Some(head), None) => loop(rem.tail, Some(head))
        case _ => acc
      }
    }

    loop(values, Some(initialValue))
  }

  println(map(numbers, x => x * 2))
  println(map_v2(numbers, _ * 2))
  println(flatMap(numbers, x => List.fill(2)(x)))
  println(flatMap_v2(numbers, List.fill(2)(_)))
  println(filter(numbers, x => x % 2 == 0))
  println(filter_v2(numbers, _ % 2 == 0))
  println(reduceLeft(numbers, (x, y) => x + y))
  println(reduceLeft_v2(numbers, _ + _))
  println(foldLeft(numbers, 1, (x, y) => x * y))
  println(foldLeft_v2(numbers, 1, _ * _))
}
