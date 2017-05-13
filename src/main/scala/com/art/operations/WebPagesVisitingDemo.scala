package com.art.operations

import com.art.SparkInitializer
import org.apache.spark.SparkContext

object WebPagesVisitingDemo extends App {

  val sc: SparkContext = SparkInitializer.init("numbers-processing-demo")

  val visits = sc.parallelize(Seq(
    ("index.html", "1.2.3.4"),
    ("about.html", "3.4.5.6"),
    ("index.html", "22.22.22.22"),
    ("faq.html", "1.2.3.4")
  ))

  val pageNames = sc.parallelize(Seq(
    ("index.html", "Home"),
    ("about.html", "About"),
    ("faq.html", "FAQ")
  ))

  val joined = visits.join(pageNames).collect().toList
  val cogrouped = visits.cogroup(pageNames).collect().toList

  println(s"Joined: $joined")
  println(s"Cogrouped: $cogrouped")

  sc.stop()
}
