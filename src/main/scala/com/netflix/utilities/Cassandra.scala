package com.netflix.utilities

import com.datastax.driver.core.{ResultSet, Row}
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.rdd.ReadConf
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
    
  //Creating a New Cassandra Table From a Dataset Schema
  import spark.implicits._
  val a = sc.parallelize(Array((1,"Sankar"),(2,"Puja"),(3,"Amazon"))).toDF("ID","Name")

  //it will create table testing2
  a.createCassandraTable("sparktocassandra","testing",partitionKeyColumns = Some(Seq("Name")))

  //setting cassandra properties through spark
  spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))
    
  }

}
