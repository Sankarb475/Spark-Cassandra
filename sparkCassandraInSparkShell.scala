//in-propgress
//while running spark-shell make sure you have given the jar details :: spark-cassandra-connector_2.11-2.0.10.jar

.spark-shell --jars /Users/sankar.biswas/Desktop/spark-cassandra-connector_2.11-2.0.10.jar

scala> import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf
import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

scala> import org.apache.spark.sql.{SQLContext, SparkSession}, org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

scala> val conf = new SparkConf().setAppName("APP_NAME").setMaster("local").set("spark.cassandra.connection.host", "localhost").set("spark.cassandra.auth.username", "").set("spark.cassandra.auth.password", "")
conf: org.apache.spark.SparkConf = org.apache.spark.SparkConf@7b3feb26

scala> val spark = SparkSession.builder().getOrCreate()
spark: org.apache.spark.sql.SparkSession = org.apache.spark.sql.SparkSession@4423692a

scala> val sc: SparkContext = spark.sparkContext
sc: org.apache.spark.SparkContext = org.apache.spark.SparkContext@7a74672

//In my local cassandra I already have created a Keyspace : "datatable" and a Table "data"

cqlsh:datatable> DESC TABLE datatable.data;

CREATE TABLE datatable.data (
    name text PRIMARY KEY,
    email list<text>
)

cqlsh:datatable> SELECT * FROM datatable.data;

 name   | email
--------+-------------------------------------------------------------------
 Sankar | ['sankarb475@gmail.com', 'bsankar207@gmail.com', 'xyz@apple.com']
   Puja |                        ['pujajha5912@gmail.com', 'cba@yahoo.com']
