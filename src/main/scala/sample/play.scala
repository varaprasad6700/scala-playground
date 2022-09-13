package com.techsophy
package sample

import java.util
import scala.util.Random

object play extends App {
  val a = new Function2[Int, Int, Int]{
    override def apply(v1: Int, v2: Int): Int = v1 + v2
  }
  def b = new Function2[Int, Int, Int]{
    override def apply(v1: Int, v2: Int): Int = v1 + v2
  }

  val c = (x: Int, y: Int) => x + y

  println(a(10, 20))
  println(b(10, 20))
  println(c(10, 20))


  def returnList: util.LinkedList[Option[Int]] = {
    val list = new util.LinkedList[Option[Int]]
    for (_ <- 0 until 10){
      val temp = Random.nextInt(10)
      if (temp < 8) {
        list.add(Some(temp))
      } else {
        list.add(None)
      }
    }
    list
  }

  val list1: util.LinkedList[Option[Int]] = returnList
  println(list1)
  collection.LinearSeq
  val list = list1
    .stream()
    .map(_.map(_ * 2))
    .filter(_.isDefined)
    .map(_.get)
    .toList
  println(list)
}
