/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object Filez {

  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = new HiveContext(sc)
    import org.apache.spark.sql.hive.online.OnlineSQLConf._
    import org.apache.spark.sql.hive.online.OnlineSQLFunctions._
    import java.io.{File, PrintWriter}
    val numPartitions = 100
    sqlContext.setConf(STREAMED_RELATIONS, "students")
    sqlContext.setConf(NUMBER_BATCHES, "100")
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, "100")
    val poolName = "vappa"
    sc.setLocalProperty("spark.scheduler.pool", poolName)
    val df = sqlContext.read.json("data/students.half.json")
    val newDF = sqlContext.createDataFrame(
      df.rdd.repartition(numPartitions), df.schema)
    newDF.registerTempTable("students")
    val odf = sqlContext.sql("SELECT AVG(uniform) FROM students").online
    odf.hasNext
    val result = (1 to odf.progress._2)
      .map { i => assert(odf.hasNext); odf.collectNext() }
    val resultString = result.map { r =>
      (r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(0),
      r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(1),
      r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(2)) }
    .zipWithIndex.map { case ((a, b, c), i) => s"${i + 1} $a $b $c" }.mkString("\n")
    // Write to file
    val pw = new PrintWriter(new File("output.dat"))
    try {
      pw.write(resultString + "\n")
    } finally {
      pw.close()
    }
  }

}
