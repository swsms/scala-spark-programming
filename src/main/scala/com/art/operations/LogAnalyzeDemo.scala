package com.art.operations

import com.art.{FileLoader, SparkInitializer}
import org.apache.spark.SparkContext

object LogAnalyzeDemo extends App {

  val sc: SparkContext = SparkInitializer.init("log-analyze-demo")

  val text = sc.textFile(FileLoader.pathToFile("log.txt"))

  val date = "2017-05-14"

  val errorsCount = text
    .filter(line => line.startsWith(date) && line.contains("ERROR"))
    .count()

  println(s"errors count $date: $errorsCount")
}
