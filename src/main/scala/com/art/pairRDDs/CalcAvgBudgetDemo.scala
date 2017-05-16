package com.art.pairRDDs

import com.art.{FileLoader, SparkInitializer}

object CalcAvgBudgetDemo extends App {

  case class Event(org: String, name: String, budget: Long)

  private def loadEvent(event: String): Event = {
    val pair = event.split(",")
    Event(pair(0), pair(1), pair(2).toLong)
  }

  val sc = SparkInitializer.init("calc-avg-budget-demo")

  // loading all events
  val eventsRDD = sc.textFile(FileLoader.pathToFile("events.csv"))
    .map(e => loadEvent(e))       // events RDD
    .map(e => (e.org, e.budget))  // K-V RDD

  val intermediate = eventsRDD
    .mapValues(b => (b, 1))       // (name, (budget, 1))
    .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))

  val avgBudgets = intermediate.mapValues {
    case (budget, numberOfEvents) => budget / numberOfEvents
  }

  avgBudgets.collect().foreach(println)
}
