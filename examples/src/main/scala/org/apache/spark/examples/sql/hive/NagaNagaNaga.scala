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

import org.apache.spark.{Logging, PoolReweighterLoss, SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._


object NagaNagaNaga extends Logging {

  private def utilFunc(time: Long, qual: Double): Double = 0.0

  private def makeThread(sqlContext: SQLContext, name: String): Thread = {
    new Thread {
      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        sc.setLocalProperty("spark.scheduler.pool", name)
        PoolReweighterLoss.startTime(name)
        PoolReweighterLoss.start(5)
        PoolReweighterLoss.register(name, utilFunc)
        sc.addSchedulablePool(name, 0, 1)
        val numPartitions = sc.getConf.get("spark.naga.numPartitions", "400").toInt
        val inputFile = sc.getConf.get("spark.naga.inputFile", "data/students.json")
        val avgColumn = sc.getConf.get("spark.naga.avgColumn", "uniform")
        val tableName = name
        val df = sqlContext.read.json(inputFile)
        val newDF = sqlContext.createDataFrame(
          df.rdd.repartition(numPartitions), df.schema)
        newDF.registerTempTable(tableName)
        val odf = sqlContext.sql(s"SELECT AVG($avgColumn) FROM $tableName").online
        odf.hasNext // DON'T DELETE THIS LINE
        val result = (1 to odf.progress._2).map { i =>
          assert(odf.hasNext)
          odf.collectNext()
        }
        val resultString = result.map { r =>
           (r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(0),
            r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(1),
            r(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(2))
          }
          .zipWithIndex
          .map { case ((a, b, c), i) => s"${i + 1} $a $b $c" }
          .mkString("\n")
        logInfo(
          "\n\n================================================================\n" +
            s"POOL($name)\n" + resultString +
          "\n================================================================\n\n"
        )
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Naga")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val numBatches = sqlContext.getConf(NUMBER_BATCHES, "100")
    val streamedRelations = sqlContext.getConf(STREAMED_RELATIONS, "naga1,naga2")
    val numBootstrapTrials = sqlContext.getConf(NUMBER_BOOTSTRAP_TRIALS, "500")
    val waitPeriod = conf.get("spark.naga.waitPeriodMs", "5000").toLong
    sqlContext.setConf(STREAMED_RELATIONS, streamedRelations)
    sqlContext.setConf(NUMBER_BATCHES, numBatches)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, numBootstrapTrials)
    val poolNames = streamedRelations.split(",")
    val threads = poolNames.map { name => makeThread(sqlContext, name) }
    threads.foreach { t =>
      t.start()
      Thread.sleep(waitPeriod)
    }
    threads.foreach { t => t.join() }
    sc.stop()
  }

}
