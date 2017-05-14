package com.art.сalculations

import com.art.SparkInitializer
import scala.math.random

object CalcPiDemo extends App {

  val sc = SparkInitializer.init("calc-pi-demo")

  val n = 100000

  val count = sc.parallelize(Range(1, n, 1)).map(i => {
    val x = random * 2 - 1
    val y = random * 2 - 1
    if (x * x + y * y < 1) 1 else 0
  }).reduce(_+_)

  println(4.0 * count / n)

  sc.stop()
}
