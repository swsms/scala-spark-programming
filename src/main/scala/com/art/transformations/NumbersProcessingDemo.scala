package com.art.transformations

import com.art.SparkInitializer
import org.apache.spark.SparkContext

object NumbersProcessingDemo extends App {

  val sc: SparkContext = SparkInitializer.init("numbers-processing-demo")

  val input = sc.parallelize(List(1, 2, 3))
  val numbers = input
    .map(n => n * n)  // {1, 4, 9}
    .filter(n => n % 2 == 0) // {4}
    .flatMap(n => Range(0, n, 1)) // {0, 1, 2, 3}
    .collect()
    .toList

  println(s"squares: $numbers")

  sc.stop()
}
