package com.techsophy
package s99

import scala.annotation.tailrec

object Problems extends App {

  //P01
  @tailrec
  def last[T](l: List[T]): T = {
    l match {
      case lastEle :: Nil => lastEle
      case _ :: tail => last(tail)
      case _ => throw new NoSuchElementException
    }
  }

  //P02
  @tailrec
  def penultimate[T](list: List[T]): T = {
    list match {
      case pen :: _ :: Nil => pen
      case _ :: tail => penultimate(tail)
      case _ => throw new NoSuchElementException
    }
  }

  //P03
  @tailrec
  def nth[T](n: Int, list: List[T]): T = {
    (n, list) match {
      case (0, ele :: _) => ele
      case (_, _ :: tail) => nth(n - 1, tail)
      case (_, Nil) => throw new NoSuchElementException
    }
  }

  //P04
  def length[T](value: List[T]): Int = {
    @tailrec
    def loop(rem: List[T], length: Int = 0): Int = {
      rem match {
        case Nil => length
        case _ :: tail => loop(tail, length + 1)
      }
    }

    loop(value)
  }

  //P05
  def reverse[T](list: List[T]): List[T] = {
    @tailrec
    def loop(rem: List[T], acc: List[T] = List.empty[T]): List[T] = {
      rem match {
        case Nil => acc
        case head :: tail => loop(tail, head :: acc)
      }
    }

    loop(list)
  }

  //P06
  def isPalindrome(list: List[Int]): Boolean = {
    list == list.reverse
  }

  //P07
  def flatten(list: List[Any]): List[Any] = {
    list.flatMap({
      case a: List[_] => flatten(a)
      case ele => List(ele)
    })
  }

  //P08
  def compress[T](list: List[T]): List[T] = {
    @tailrec
    def loop(rem: List[T], acc: List[T] = List.empty[T]): List[T] = {
      rem match {
        case Nil => acc.reverse
        case head :: tail => loop(tail.dropWhile(_ == head), head :: acc)
      }
    }

    loop(list)
  }

  //P09
  def pack[T](list: List[T]): List[List[T]] = {
    @tailrec
    def loop(acc: List[List[T]] = List.empty, rem: List[T]): List[List[T]] = {
      rem match {
        case Nil => acc.reverse
        case head :: _ => loop(rem.takeWhile(_ == head) :: acc, rem.dropWhile(_ == head))
      }
    }

    loop(rem = list)
  }

  println(s"PO1 ${last(List(1, 1, 2, 3, 5, 8))}")

  println(s"PO2 ${penultimate(List(1, 1, 2, 3, 5, 8))}")

  println(s"P03 ${nth(2, List(1, 1, 2, 3, 5, 8))}")

  println(s"P04 ${length(List(1, 1, 2, 3, 5, 8))}")

  println(s"P05 ${reverse(List(1, 1, 2, 3, 5, 8))}")

  println(s"P06 ${isPalindrome(List(1, 2, 3, 2, 1))}")

  println(s"P07 ${flatten(List(List(1, 1), 2, List(3, List(5, 8))))}")

  println(s"P08 ${compress(List(Symbol("a"), Symbol("a"), Symbol("a"), Symbol("a"), Symbol("b"), Symbol("c"), Symbol("c"), Symbol("a"), Symbol("a"), Symbol("d"), Symbol("e"), Symbol("e"), Symbol("e"), Symbol("e")))}")

  println(s"P09 ${pack(List(Symbol("a"), Symbol("a"), Symbol("a"), Symbol("a"), Symbol("b"), Symbol("c"), Symbol("c"), Symbol("a"), Symbol("a"), Symbol("d"), Symbol("e"), Symbol("e"), Symbol("e"), Symbol("e")))}")
}
