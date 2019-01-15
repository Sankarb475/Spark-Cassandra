package com.netflix.utilities

import com.datastax.driver.core.{ResultSet, Row}
//import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


class Cassandra {
  def main(args: Array[String]) {
  val conf = new SparkConf().setAppName("APP_NAME")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "localhost")
    .set("spark.cassandra.auth.username", "")
    .set("spark.cassandra.auth.password", "")

  val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

  val sc: SparkContext = spark.sparkContext

  val data = sc.cassandraTable("datatable", "data")

  println(data.collect.foreach(println))
  }

}
