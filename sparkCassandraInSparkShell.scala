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
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99PERCENTILE';

cqlsh:datatable> SELECT * FROM datatable.data;

 name   | email
--------+-------------------------------------------------------------------
 Sankar | ['sankarb475@gmail.com', 'bsankar207@gmail.com', 'xyz@apple.com']
   Puja |                        ['pujajha5912@gmail.com', 'cba@yahoo.com']


scala> val data = sc.cassandraTable("datatable", "data")
warning: Class org.joda.convert.FromString not found - continuing with a stub.
data: com.datastax.spark.connector.rdd.CassandraTableScanRDD[com.datastax.spark.connector.CassandraRow] = CassandraTableScanRDD[0] at RDD at CassandraRDD.scala:19
