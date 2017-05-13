package examples

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld extends App {
  val master = "local[3]"
  val appName = "hello-world"

  val conf: SparkConf = new SparkConf()
    .setAppName(appName)
    .setMaster(master)

  val sc: SparkContext = new SparkContext(conf)

  val helloRDD = sc.parallelize(List("World", "Scala", "Spark"))
  val list = helloRDD.map(word => "Hello, " + word).collect().toList

  list.foreach(hello => println(hello))

  sc.stop()
}
