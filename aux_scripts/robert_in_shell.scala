
val batch_count = 10
val partition_count = 100
val input_filename = "data/students.json"

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner


import sqlContext.implicits._
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._


sqlContext.setConf(STREAMED_RELATIONS, "students")
sqlContext.setConf(NUMBER_BATCHES, batch_count.toString)
/*
// val schema = new StructType(Array( 
//                StructField("name", StringType, false),
//                StructField("index", LongType, false),
                StructField("coin_toss", IntegerType, false),
                StructField("normal", DoubleType, false),
                StructField("zipf", LongType, false),
                StructField("uniform", IntegerType, false),
                StructField("twin_peak", DoubleType, false),
                StructField("geometric", IntegerType, false),
                StructField("exponential", LongType, false),
                StructField("lognormal", DoubleType, false),
                StructField("zero", IntegerType, false)))
*/

//var rdd = sc.textFile("aux_scripts/interesting_data.csv").map(line=>Row.fromSeq(line.split(",")))
var df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("aux_scripts/interesting_data.csv.csv")

val schema = df.schema
val rdd = df.rdd.repartition(partition_count)
val df = sqlContext.createDataFrame(rdd, schema)
//rdd = rdd.repartition(partition_count)

//val df = sqlContext.createDataFrame(rdd,schema)
df.show()
//df.registerTempTable("students")
//val odf = sqlContext.sql("SELECT AVG(uniform) FROM students").online

//var results : Array[Array[String]] = new Array[Array[String]](10)
//for (i <- 0 to batch_count-1) {
//	val subresult = odf.collectNext() // Should be an array of 3 values: [Approx Ans, Lower Conf, Upper Conf]
//	results(i) = subresult.map(x -> x.toString)
//}