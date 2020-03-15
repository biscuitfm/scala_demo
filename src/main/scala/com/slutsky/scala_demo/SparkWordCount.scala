package com.slutsky.scala_demo

import com.slutksy.logging.Logger.info
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  private val master = "spark://Master:7077"
  private val remote_file = "hdfs://Master:8020/shumengke/hadoop/dir1/mptest.info"
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(remote_file)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
    sc.stop()
    new Test
  }

  class Project(name: String, daysToComplete: Int)

  class Test {
    var project1 = new Project("TPS Reports", 1)
    var project2 = new Project("Website redesign", 5)
    info("Created projects")
  }

}