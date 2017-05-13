package com.art.operations

import com.art.{FileLoader, SparkInitializer}
import org.apache.spark.SparkContext

object WordCountDemo extends App {

  val sc: SparkContext = SparkInitializer.init("word-count-demo")

  val text = sc.textFile(FileLoader.pathToFile("words.txt"))

  val wc = text.map(word => (word, 1))
    .reduceByKey((acc, elem) => acc + elem) // there is collectByKey() exists as well
    .collect()
    .toList

  println(s"word counts: $wc")

  sc.stop()
}
