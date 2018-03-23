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

import java.io.{BufferedWriter, FileWriter}

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._

object TestIolapPR extends Logging {
  def makeThread(sqlContext: HiveContext, name: String): Thread = {
    new Thread {
      val sc = sqlContext.sparkContext
      val avgColumn = sc.getConf.get("spark.slaq.avgColumn", "uniform")
      val logDir = sc.getConf.get("spark.approx.logDir",
        "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
      val idx = (name.charAt(name.size - 1).toInt - 1) % 3
      val tableName = "table" + idx
      val odf = sqlContext
        .sql(s"SELECT AVG($avgColumn) FROM $tableName GROUP BY fivegroup").online
//      val queries = Array(s"SELECT AVG(normal) FROM $tableName", "", "")
//      val odf = sqlContext
//        .sql(s"SELECT AVG(colA) FROM (SELECT $tableName.normal" +
//          s" as colA, t.fivegroup from $tableName JOIN " +
//          s"nonStream AS t ON $tableName.index = t.index " +
//          s") cols").online
//      logInfo("\n\n\n\nLOGAN: Before PREPARE\n\n\n\n")
      odf.prepareDataFrames()
//      logInfo("\n\n\n\nLOGAN: After PREPARE\n\n\n\n")

      override def run(): Unit = {
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        var prevLoss = 0.0
        var initialDLoss = 0.0
        val result = (1 to odf.progress._2).map { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: time ${System.currentTimeMillis() - t1}")
          var lossSum = 0.0
          var currentResult = ""
          col.foreach { c =>
            val avg = c.get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(0)
            val low = c.get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(1)
            val high = c.get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(2)
            currentResult += "[" + low + "," + avg + "," + high + "]\n"
            val loss = high - low
            lossSum += loss
          }
          val bw = new BufferedWriter(new FileWriter(logDir + name + ".output"))
          bw.write(currentResult.trim())
          bw.close()
          val loss = lossSum / col.size
          var dLoss = prevLoss - loss
          prevLoss = loss
          if (initialDLoss == 0.0) {
            initialDLoss = -dLoss
            dLoss = 1.0
          } else {
            dLoss = dLoss / initialDLoss
          }
          val isFair = sc.getConf.get("spark.slaq.isFair", "false").equals("true")
          if (!isFair) {
            sc.setPoolWeight(name, (dLoss * 10000).toInt)
          } else {
            sc.setPoolWeight(name, 1)
          }
          logInfo(s"LOGAN: $name $dLoss")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestIolap")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    val numPools = sc.getConf.get("spark.slaq.numPools", "1").toInt
    val poolNames = (1 to numPools).map( x => s"t$x" ).toArray
    val numBatches = sc.getConf.get("spark.slaq.numBatches", "40")
    val streamedRelations = poolNames.mkString(",")
    val numBootstrapTrials = "200"
    // val waitPeriod = 60000
    val waitPeriod = 0

    val numPartitions = sc.getConf.get("spark.slaq.numPartitions", "16000").toInt
//    val inputFile = sc.getConf.get("spark.slaq.inputFile", "data/students5g.json")
//    val inputFiles = Array("data/students0.5g.json", "data/students.json", "data/students5g.json")
//    val inputFiles = Array("data/students0.5g.json", "/disk/local/disk2/stafman/students30g_2.json")
    // comma separated list of input files
    val inputFiles = sc.getConf.get("spark.approx.inputFiles",
      "/disk/local/disk2/stafman/students30g_2.json").split(",")
//    val inputFiles = Array("data/students5g.json")
    val regDF = sqlContext.read.json(inputFiles(0))
    val dfs = (0 until inputFiles.length).map(x => sqlContext.read.json(inputFiles(x)))
    val newDFs = (0 until inputFiles.length).map(x => sqlContext.createDataFrame(
      dfs(x).rdd.repartition(numPartitions), dfs(x).schema))
    val nsd = sqlContext.createDataFrame(regDF.rdd.repartition(numPartitions), regDF.schema)
    (0 until inputFiles.length).foreach{ x => newDFs(x).registerTempTable("table" + x) }
    (0 until inputFiles.length).foreach{ x => sqlContext.cacheTable("table" + x); sqlContext.sql(s"SELECT COUNT(*) FROM table$x").collect() }
//    nsd.cache()
//    newDFs.map(x => x.cache())
//    nsd.registerTempTable("nonStream")
//    sqlContext.cacheTable("nonStream")
//    nsd.count()
//    newDFs.map(x => x.count())
//    sqlContext.table("table").withColumn(SEED_COLUMN, new Column(RandomSeed()))
//     sqlContext.cacheTable("table")
    val streamedRels = (0 until inputFiles.length).map( x => s"table$x").toArray.mkString(",")
    sqlContext.setConf(STREAMED_RELATIONS, streamedRels)
    sqlContext.setConf(NUMBER_BATCHES, numBatches)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, numBootstrapTrials)
    val threads = poolNames.map { name => makeThread(sqlContext, name) }
    threads.foreach { t =>
      t.start()
      Thread.sleep(waitPeriod)
    }
    threads.foreach { t => t.join() }
  }
}
