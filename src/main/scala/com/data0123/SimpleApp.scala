package com.data0123

import org.apache.spark.sql.SparkSession

/**
  * Created by data on 2017/11/15.
  */
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "Y:\\Noname8.html" // Should be some file on your system
    val spark = SparkSession.builder
      .master("local").appName("Simple Application")
      .getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}
