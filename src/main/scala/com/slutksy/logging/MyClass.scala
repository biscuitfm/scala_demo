package com.slutksy.logging

import org.apache.spark.rdd.RDD

class MyClass extends Serializable {
  val field = "Hello"
  def func1(s: String): String = {field + " " + s}
  def doStuff(rdd: RDD[String]): RDD[String] = { rdd.map(func1) }

}
