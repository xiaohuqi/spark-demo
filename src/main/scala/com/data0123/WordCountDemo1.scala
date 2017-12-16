package com.data0123

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

/**
  * Created by data on 2017/12/13.
  */
object WordCountDemo1 {

  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    val rdd0 = spark.read.textFile("Z:/txt/fhwz.txt").cache()
    val rdd1 = rdd0.filter(line => line.contains("胡斐"))
    val rdd2 = rdd0.filter(line => line.contains("程灵素"))
    val rdd12 = rdd1.union(rdd2)
    println(rdd1.count())
    println(rdd2.count())
    println(rdd12.count())

    val rdd00 = rdd0.rdd.map(line => line.length)

//    println(rdd3.count())
    println(rdd00.reduce((a, b) => if(a > b) a else b))
    println(rdd00.reduce((a, b) => a + b))


//    var sum = new LongAccumulator();
    val sum = spark.sparkContext.longAccumulator("sum");
    rdd00.foreach(x => sum.add(x));
    println(sum)

//    rdd00.foreach(println);

    var mapRdd0 = rdd0.rdd.map(line => (line.substring(0, 1), line.length))
//    mapRdd0.reduceByKey((x, y) => x + y).foreach(println)
//    mapRdd0.groupByKey().foreach(println)
//    mapRdd0.foreach(println)

//    val rdd4 = rdd1.rdd.flatMap(line => line.split("，"))
//    println(rdd4.count())
//
//    val result = rdd0.rdd.map(line => line.length).aggregate((0, 0))(
//      (acc, value) => (acc._1 + value, acc._2 + 1),
//      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
//    )
//    println(result._1 / result._2.toDouble)


//    var rdd5 = rdd1.distinct()
//    println(rdd5.count())

    spark.close();
  }


}
