package com.slutksy.logging

object Logger {
  def info(message: String): Unit = println(s"INFO: $message")
}