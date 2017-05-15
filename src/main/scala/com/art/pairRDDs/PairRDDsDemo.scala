package com.art.pairRDDs

import com.art.{FileLoader, SparkInitializer}

object PairRDDsDemo extends App {

  case class User(login: String, firstName: String, lastName: String, phone: String, country: String)

  val sc = SparkInitializer.init("pair-rdds-demo")

  val users = sc.textFile(FileLoader.pathToFile("users.csv"))

  val groupedByCountry = users.map(line => {
    val values = line.split(",")
    (values(4), values(0))
  }).groupByKey()
    .collect()
    .toList

  println(s"grouped by country $groupedByCountry")

  sc.stop()
}
