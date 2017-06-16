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

import java.io.{File, PrintWriter}

import org.apache.spark.{PoolReweighterLoss, SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._

object RobertInTheFile {
  private val tableName = "students"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    // Some confs
    val inputPath = conf.get("spark.naga.inputPath", "data/students.json")
    val intervalMs = conf.get("spark.naga.intervalMs", "5000").toLong
    val numPartitions = conf.get("spark.naga.numPartitions", "1000").toInt
    val numPools = conf.get("spark.naga.numPools", "10").toInt
    val streamedRelations = sqlContext.getConf(STREAMED_RELATIONS, tableName)
    val numBatches = sqlContext.getConf(NUMBER_BATCHES, "100")
    val numBootstrapTrials = sqlContext.getConf(NUMBER_BOOTSTRAP_TRIALS, "100")
    sqlContext.setConf(STREAMED_RELATIONS, streamedRelations)
    sqlContext.setConf(NUMBER_BATCHES, numBatches)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, numBootstrapTrials)
    // Make some threads, one per streamed relation
    val df = sqlContext.read.json(inputPath)
    sqlContext
      .createDataFrame(df.rdd.repartition(numPartitions), df.schema)
      .registerTempTable(tableName)
    // sqlContext.cacheTable(tableName)
    // sqlContext.table(tableName).count() // materialize
    val threads = (1 to numPools).map(makeThread)
    // Slaq conf
    val slaqEnabled = conf.get("spark.slaq.enabled", "true").toBoolean
    val slaqIntervalMs = conf.get("spark.slaq.intervalMs", "5000").toLong
    PoolReweighterLoss.start((slaqIntervalMs / 1000).toInt, fair = !slaqEnabled)
    threads.foreach { t =>
      t.start()
      Thread.sleep(intervalMs)
    }
    threads.foreach(_.join())
    PoolReweighterLoss.stop()
  }

  private def makeThread(poolNum: Int): Thread = {
    new Thread {
      override def run(): Unit = {
        val sc = SparkContext.getOrCreate()
        val sqlContext = SQLContext.getOrCreate(sc)
        val poolName = "pool" + poolNum
        sc.setLocalProperty("spark.scheduler.pool", poolName)
        sc.addSchedulablePool(poolName, 0, 1)
        // Some confs
        val conf = sc.getConf
        val numPartitions = conf.get("spark.naga.numPartitions", "1000").toInt
        val selectArg = conf.get("spark.naga.selectArg", "AVG(uniform)")
        val inputPath = conf.get("spark.naga.inputPath", "data/students.json")
        val outputDir = conf.get("spark.naga.outputDir", ".")
        val outputSuffix = conf.get("spark.naga.outputSuffix", "")
        val outputPath =
          if (outputSuffix.nonEmpty) {
            s"$outputDir/$poolName.$outputSuffix.dat"
          } else {
            s"$outputDir/$poolName.dat"
          }
        PoolReweighterLoss.register(poolName)
        // Run IOLAP
        val odf = sqlContext.sql(s"SELECT $selectArg FROM $tableName").online
        odf.hasNext // DO NOT REMOVE THIS LINE!
        val (_, numTotalBatches) = odf.progress
        val resultString = (1 to numTotalBatches).map { _ =>
          assert(odf.hasNext)
          val r = odf.collectNext()
          val currentTime = System.currentTimeMillis()
          val answer = r(0).get(0).asInstanceOf[Row].getDouble(0)
          val lower = r(0).get(0).asInstanceOf[Row].getDouble(1)
          val upper = r(0).get(0).asInstanceOf[Row].getDouble(2)
          s"$currentTime $answer $lower $upper"
        }.mkString("\n")
        println(
          "\n\n\n**************************************\n" +
          s"(data for '$poolName')\n" +
          resultString +
          "\n**************************************\n\n\n")
        ensureDirExists(outputDir)
        writeToFile(resultString, outputPath)
      }
    }
  }

  private def ensureDirExists(path: String): Unit = {
    new File(path).mkdirs()
  }

  private def writeToFile(content: String, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))
    try {
      writer.write(content + "\n")
    } finally {
      writer.close()
    }
  }

}
