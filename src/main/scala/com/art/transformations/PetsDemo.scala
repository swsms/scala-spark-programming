package com.art.transformations

import com.art.SparkInitializer
import org.apache.spark.SparkContext

object PetsDemo extends App {

  val sc: SparkContext = SparkInitializer.init("numbers-processing-demo")

  val pets = sc.parallelize(Seq(("cat", 1), ("dog", 1), ("cat", 2), ("mouse", 2)))

  val reducedPets = pets.reduceByKey((acc, elem) => acc + elem).collect().toList
  val groupedPets = pets.groupByKey().collect().toMap

  println(s"reduced pets: $reducedPets")
  println(s"grouped pets: $groupedPets")

  sc.stop()
}
