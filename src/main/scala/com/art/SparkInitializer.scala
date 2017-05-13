package com.art

import org.apache.spark.{SparkConf, SparkContext}

object SparkInitializer {
  val master = "local[3]" // local with 3 threads

  def init(appName: String) = {
    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(master)
    new SparkContext(conf)
  }

}
