
val batch_count = 100
val partition_count = 400
val query = "SELECT AVG(twin_peak) FROM students"


val input_filename = "data/students.json"
val output_filename = "data/students.dat"

import java.io._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.HashPartitioner
import sqlContext.implicits._
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._


sqlContext.setConf(STREAMED_RELATIONS, "students")
sqlContext.setConf(NUMBER_BATCHES, batch_count.toString)

var df = sqlContext.read.json(input_filename)
df = df.repartition(partition_count)

val schema = df.schema
val rdd = df.rdd.repartition(partition_count)
val df = sqlContext.createDataFrame(rdd, schema)

//df.show()
df.registerTempTable("students")
val odf = sqlContext.sql(query).online

val pw = new PrintWriter(new File(output_filename))
{
        odf.hasNext
        val result = (1 to odf.progress._2).map { i => assert(odf.hasNext); odf.collectNext() }
        result.map { r => 
          (r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(0),
          r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(1),
          r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(2)) }.zipWithIndex.foreach { case ((a, b, c), i) => pw.println(s"${i+1} $a $b $c") }
}
pw.close()