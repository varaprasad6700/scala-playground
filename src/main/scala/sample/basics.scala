package com.techsophy
package sample

object basics extends App {
  //  1. Setup project in intellij, create git repo and push your code to github.
  //  2. Write a function that takes two boolean inputs and returns XOR of it.

  def xor(a: Boolean, b: Boolean): Boolean = a ^ b

  //  3. Write a function that takes an int and prints whether the number is even or odd.

  def isEven(num: Int): Boolean = num % 2 == 0
  //  4. Write a function that takes an int value and returns true if it’s a prime number, otherwise false.

  //  5. Write a function that takes an int value and returns factorial of it using while loop.
  def factorial(num: Int): Long = {
    var i = 1
    var fact: Long = 1
    while( i <= num) {
      fact *= i
      i += 1
    }
    fact
  }
  //  6. Perform (3) using tail recursion.
  //  7. Write a function that takes a number N and returns the Nth value of the fibonacci number.
  def fibonacci(n: Int): Int ={
    if (n <= 2) n - 1
    else fibonacci(n - 1) + fibonacci(n - 2)
  }
  //  8. Perform (4) using tail recursion.
  println(xor(a = true, b = true))
  println(xor(a = true, b = false))
  println(isEven(5))
  println(isEven(6))
  println(factorial(4))
  println(s"fibonacci(5) = ${fibonacci(5)}")
}
