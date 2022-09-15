package com.techsophy
package collections

object Part4 extends App {
  val numbers = List(1, 2, 3, 4, 5)
  //  PART 4
  //  1. Implement map using flatMap
  println(numbers.flatMap(n => List(List.fill(2)(n))))
  //  2. Implement map using foldLeft
  numbers.foldLeft[List[Int]](List.empty[Int])((li, ele) => ele :: li)
  //  3. Implement flatMap using foldLeft
  //  4. Implement reduceLeft using foldLeft
//  Second Minimum element from a list
}
