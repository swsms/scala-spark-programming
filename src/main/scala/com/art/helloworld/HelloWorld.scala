package com.art.helloworld

import com.art.SparkInitializer
import org.apache.spark.SparkContext

object HelloWorld extends App {

  val sc: SparkContext = SparkInitializer.init("hello-world")

  val helloRDD = sc.parallelize(List("World", "Scala", "Spark"))
  val list = helloRDD.map(word => "Hello, " + word).collect().toList

  list.foreach(hello => println(hello))

  sc.stop()
}
