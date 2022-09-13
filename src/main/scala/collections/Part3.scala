package com.techsophy
package collections

import scala.annotation.tailrec

object Part3 extends App {
  val numbers = List(1, 2, 3, 4, 5)

  //  PART 3
  //  1. Implement generic map using tail recursion - def map[A, B](values: List[A], f: A => B): List[B]
  def map[A, B](values: List[A], f: A => B): List[B] = {
    @tailrec
    def loop(rem: List[A], acc: List[B] = List.empty[B]): List[B] = {
      rem.headOption match {
        case None => acc
        case Some(head) => loop(rem.tail, f(head) :: acc)
      }
    }

    loop(values)
  }

  //  2. Implement generic flatMap using tail recursion - def flatMap[A, B](values: List[A], f: A => List[B]): List[B]
  def flatMap[A, B](values: List[A], f: A => List[B]): List[B] = {
    @tailrec
    def loop(rem: List[A], acc: List[B] = List.empty[B]): List[B] = {
      rem.headOption match {
        case None => acc
        case Some(head) => loop(rem.tail, f(head) ::: acc)
      }
    }

    loop(values).reverse
  }

  //  4. Implement generic filter using tail recursion - def filter[A](values: List[A], f: A => Boolean): List[A]
  def filter[A](values: List[A], f: A => Boolean): List[A] = {
    @tailrec
    def loop(rem: List[A], acc: List[A] = List.empty[A]): List[A] = {
      rem.headOption match {
        case None => acc
        case Some(head) if f(head) => loop(rem.tail, head :: acc)
        case _ => loop(rem.tail, acc)
      }
    }

    loop(values).reverse
  }

  //  5. Implement generic reduceLeft using tail recursion - def reduceLeft[A](values: List[A], f: (A, A) => A): List[A]
  def reduceLeft[A](values: List[A], f: (A, A) => A): Option[A] = {
    @tailrec
    def loop(rem: List[A], acc: Option[A] = None): Option[A] = {
      (rem.headOption, acc) match {
        case (Some(head), Some(reduced)) => loop(rem.tail, Some(f(head, reduced)))
        case (Some(head), None) => loop(rem.tail, Some(head))
        case _ => acc
      }
    }

    loop(values, None)
  }

  //  6. Implement generic foldLeft using tail recursion - def foldLeft[A, B](values: List[A], initialValue: B, f: (B, A) => B): B
  def foldLeft[A, B](values: List[A], initialValue: B, f: (B, A) => B): Option[B] = {
    @tailrec
    def loop(rem: List[A], acc: Option[B] = None): Option[B] = {
      (rem.headOption, acc) match {
        case (Some(head), Some(reduced)) => loop(rem.tail, Some(f(reduced, head)))
        case _ => acc
      }
    }

    loop(values, Some(initialValue))
  }

  println(map[Int, String](numbers, _.toString * 2))
  println(flatMap[Int, Double](numbers, List.fill(2)(_)))
  println(filter[Int](numbers, _ % 2 == 0))
  println(reduceLeft[Int](numbers, _ + _))
  println(foldLeft[Int, String](numbers, "str", _ + _))
}
