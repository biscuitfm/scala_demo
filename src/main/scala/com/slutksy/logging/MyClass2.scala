package com.slutksy.logging

import org.apache.spark.rdd.RDD

class MyClass2 {
  val field = "Hello"
  def doStuff(rdd: RDD[String]): RDD[String] = {
    val field_ = this.field
    rdd.map(line => field_ + " " + line)
  }

}
