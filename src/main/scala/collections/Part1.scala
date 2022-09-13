package com.techsophy
package collections


object Part1 extends App {
  //  1. Sum of all the numbers in the list
  val numbers = List(1, 2, 3, 4, 5)
  println(numbers.fold(0)((v1: Int, v2: Int) => v1 + v2))
  println(numbers.sum)

  //  2. Text data processing -
  //    Given: List[String] i.e. English statements
  val str = """One morning, when Gregor Samsa woke from troubled dreams, he found himself transformed in his bed into a horrible vermin. He lay on his armour-like back, and if he lifted his head a little he could see his brown belly, slightly domed and divided by arches into stiff sections. The bedding was hardly able to cover it and seemed ready to slide off any moment. His many legs, pitifully thin compared with the size of the rest of him, waved about helplessly as he looked. "What's happened to me?" he thought. It wasn't a dream. His room, a proper human room although a little too small, lay peacefully between its four familiar walls. A collection of textile samples lay spread out on the table - Samsa was a travelling salesman - and above it there hung a picture that he had recently cut out of an illustrated magazine and housed in a nice, gilded frame. It showed a lady fitted out with a fur hat and fur boa who sat upright, raising a heavy fur muff that covered the whole of her lower arm towards the viewer. Gregor then turned to look out the window at the dull weather. Drops """

  //    a. Tokenization - Split strings by a whitespace character - it will generate another string.
  val stringList: List[String] = str.split("\\s").toList
  println(stringList)

  //    b. Filter out any token containing special symbols.
  println(stringList.filter(_.matches("^[a-zA-Z0-9]*$")))

  //    c. Lower case every token.
  println(stringList.map(_.toLowerCase))

  //    d. Find frequency of every token and store them in a Map.
  val wordFrequency: Map[String, Int] = stringList.groupBy(word => word).view.mapValues(_.length).toMap
  println(wordFrequency)

  //    e. Find max frequency token.
  val maxFreqToken = wordFrequency.toList
    .sortBy(_._2)(Ordering.Int.reverse)
    .head
  println(maxFreqToken._1)

  //    f. For the Map generated in (4), divide their frequency by max value.
  println(wordFrequency.map((tuple: (String, Int)) => (tuple._1, tuple._2 / maxFreqToken._2)))


  //  3. Second minimum element from a list
  print(numbers.sorted.lift(1))

//  val (maxFreqToken, maxFreq) = wordFrequency.maxBy(_._2)
//  wordFrequency.map {case (token, freq) => (token, freq / maxFreq)}
//  wordFrequency.view.mapValues(_ / maxFreq)
}
