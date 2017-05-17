package com.art.pairRDDs

import com.art.{FileLoader, SparkInitializer}

object JoiningLogEntryAndWebSitesDemo extends App {

  case class LogEntry(userLogin: String, url: String)

  case class WebSite(name: String, url: String)

  val sc = SparkInitializer.init("joining-users-and-urls-demo")

  private def loadLogEntry(entry: String): LogEntry = {
    val pair = entry.split("\\s+")
    val e = LogEntry(pair(3), pair(4))
    return e
  }

  private def loadSite(site: String): WebSite = {
    val pair = site.split("\\s+")
    WebSite(pair(1), pair(0))
  }

  val entriesRDD = sc.textFile(FileLoader.pathToFile("log_entry.txt"))
    .map(entry => loadLogEntry(entry))
    .map(entry => (entry.url, entry.userLogin))

  val sites = sc.textFile(FileLoader.pathToFile("sites.txt"))
    .map(site => loadSite(site))
    .map(site => (site.url, site.name))

  val joined = entriesRDD
    .join(sites)
    .collect()

  joined.foreach(println)

  sc.stop()
}
