package com.slutsky.scala_demo
import com.slutksy.logging.Logger.info

/**
 * @author ${user.name}
 * 
 */

class Project(name: String, daysToComplete: Int)

class Test {
  var project1 = new Project("TPS Reports", 1)
  var project2 = new Project("Website redesign", 5)

  info("Created projects")

}

object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
    new Test
  }

}
