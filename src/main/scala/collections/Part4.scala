package com.techsophy
package collections

object Part4 extends App {
  val numbers = List(1, 2, 3, 4, 5)
  val mapFunction = (x: Int) => 2 * x
  val flatMapFunction = (n: Int) => List.fill(2)(n)
  //  PART 4
  //  1. Implement map using flatMap
  println(
    numbers
    .flatMap(n => List(mapFunction(n)))
  )

  //  2. Implement map using foldLeft
  println(
    numbers
      .foldLeft(List.empty[Int])((acc, x) => mapFunction(x) :: acc)
      .reverse
  )

  //  3. Implement flatMap using foldLeft
  println(
    numbers
      .foldLeft(List.empty[Int])((acc, x) => flatMapFunction(x) ::: acc)
      .reverse
  )

  //  4. Implement reduceLeft using foldLeft
  println(
    numbers
      .tail
      .foldLeft(numbers.head)(_ + _)
  )
}
