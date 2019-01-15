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

/*
 name   | email
--------+-------------------------------------------------------------------
 Sankar | ['sankarb475@gmail.com', 'bsankar207@gmail.com', 'xyz@apple.com']
   Puja |                        ['pujajha5912@gmail.com', 'cba@yahoo.com']
*/



scala> val data = sc.cassandraTable("datatable", "data")
warning: Class org.joda.convert.FromString not found - continuing with a stub.
data: com.datastax.spark.connector.rdd.CassandraTableScanRDD[com.datastax.spark.connector.CassandraRow] = CassandraTableScanRDD[0] at RDD at CassandraRDD.scala:19


scala> data.collect.foreach(println)
19/01/15 12:28:24 WARN NettyUtil: Found Netty's native epoll transport, but not running on linux-based operating system. Using NIO instead.
CassandraRow{name: Puja, email: [pujajha5912@gmail.com,cba@yahoo.com]}          
CassandraRow{name: Sankar, email: [sankarb475@gmail.com,bsankar207@gmail.com,xyz@apple.com]}

scala> val dataDummy = data.select("email")
dataDummy: data.Self = CassandraTableScanRDD[3] at RDD at CassandraRDD.scala:19

scala> dataDummy.collect.foreach(println)
CassandraRow{email: [pujajha5912@gmail.com,cba@yahoo.com]}
CassandraRow{email: [sankarb475@gmail.com,bsankar207@gmail.com,xyz@apple.com]}

//Another table I have in Cassandra adultdata

/*
cqlsh:datatable> SELECT * FROM adultdata limit 2;

 age | employer | salary | education  | educationyear | colour              | country        | familystatus | gender | jobdesignation | maritalstatus  | points1 | points2 | points3 | salarytype
-----+----------+--------+------------+---------------+---------------------+----------------+--------------+--------+----------------+----------------+---------+---------+---------+------------
  23 |        ? |  22966 |  Bachelors |            13 |               White |  United-States |    Own-child |   Male |              ? |  Never-married |       0 |       0 |      35 |      <=50K
  23 |        ? |  27415 |       11th |             7 |  Amer-Indian-Eskimo |  United-States |    Own-child |   Male |              ? |  Never-married |       0 |       0 |      40 |      <=50K
*/

scala> val adultdataSet = sc.cassandraTable("datatable","adultdata").select("age","salary","education")
adultdataSet: com.datastax.spark.connector.rdd.CassandraTableScanRDD[com.datastax.spark.connector.CassandraRow] = CassandraTableScanRDD[6] at RDD at CassandraRDD.scala:19

scala> adultdataSet.collect.foreach(println)
CassandraRow{age: 69, salary: 28197, education:  HS-grad}
CassandraRow{age: 69, salary: 106566, education:  Doctorate}
CassandraRow{age: 69, salary: 107575, education:  HS-grad}
CassandraRow{age: 69, salary: 111238, education:  9th}
CassandraRow{age: 69, salary: 117525, education:  Assoc-acdm}
CassandraRow{age: 69, salary: 121136, education:  HS-grad}
CassandraRow{age: 69, salary: 148694, education:  HS-grad}
CassandraRow{age: 69, salary: 159077, education:  11th}
CassandraRow{age: 69, salary: 163595, education:  HS-grad}
CassandraRow{age: 69, salary: 164102, education:  HS-grad}
CassandraRow{age: 69, salary: 167826, education:  Bachelors}
CassandraRow{age: 69, salary: 168794, education:  7th-8th}
CassandraRow{age: 69, salary: 171050, education:  HS-grad}

scala> val salarySum = adultdataSet.select("salary").as((c: Int) => c).sum
warning: Class org.joda.convert.FromString not found - continuing with a stub.
warning: Class org.joda.convert.ToString not found - continuing with a stub.
warning: Class org.joda.convert.ToString not found - continuing with a stub.
warning: Class org.joda.convert.ToString not found - continuing with a stub.
salarySum: Double = 6.078066248E9


//For further details this link would be helpful :: https://docs.datastax.com/en/datastax_enterprise/5.0/datastax_enterprise/spark/usingSparkContext.html
