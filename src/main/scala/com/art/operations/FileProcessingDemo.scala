package com.art.operations

import com.art.{FileLoader, SparkInitializer}
import org.apache.spark.SparkContext

object FileProcessingDemo extends App {

  val sc: SparkContext = SparkInitializer.init("file-processing-demo")

  val text = sc.textFile(FileLoader.pathToFile("latin.txt"))

  val result = text.flatMap(line => line.split(""))
    .filter(ch => ch >= "A" && ch <= "z")
    .map(ch => (ch, 1))
    .reduceByKey((acc, elem) => acc + elem)
    .map(x => x.swap)               // swap key and value
    .sortByKey(ascending = false)   // order by asc key
    .collect().toList

  println(s"Result: $result")

  sc.stop()
}
