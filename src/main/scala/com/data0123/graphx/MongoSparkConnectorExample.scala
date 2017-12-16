package com.data0123.graphx

import com.mongodb.spark._
import com.mongodb.spark.config._
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.bson.Document

/**
  * Created by data on 2017/12/15.
  */
object MongoSparkConnectorExample {

  def main(args: Array[String]): Unit ={
    val spark = SparkSession
      .builder.master("local").config("spark.mongodb.output.uri", "mongodb://192.168.25.1:19130/kg_ct.pagerank_result")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext
    val rdd0 = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://192.168.25.1:19130/kg_ct.attribute_object", "partitioner" -> "MongoShardedPartitioner")))

    val rdd01: RDD[(VertexId, VertexId)] = rdd0.rdd.map(document => (document.getLong("entity_id"), document.getLong("attr_value")))
    println(rdd01.count())

    val graph = Graph.fromEdgeTuples(rdd01, 1)
    val ranks = graph.pageRank(0.0001).vertices

    var documents = ranks.map(line => new Document("entity_id", line._1).append("pr_score", line._2));
    MongoSpark.save(documents)

    println(ranks.collect().mkString("\n"))

//    val rdd1 = sc.loadFromMongoDB(ReadConfig(Map("uri" -> "mongodb://192.168.25.1:19130/kg_ct.basic_info", "partitioner" -> "MongoShardedPartitioner")))
//
//
//    val users = sc.textFile("d:/work/scala/data/users.txt").map { line =>
//      val fields = line.split(",")
//      (fields(0).toLong, fields(1))
//    }
//    val ranksByUsername = users.join(ranks).map {
//      case (id, (username, rank)) => (username, rank)
//    }
//    // Print the result
//    println(ranksByUsername.collect().mkString("\n"))

    println(rdd0.first().get("entity_id"))
  }
}
