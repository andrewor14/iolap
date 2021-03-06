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
import org.apache.spark.sql.hive.online.OnlineDataFrame
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._

object RobertInTheFile {
  private val TABLE_NAME = "students"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    val sc = SparkContext.getOrCreate(conf)
    val sqlContext = new HiveContext(sc)
    setup(sqlContext)
    val intervalMs = conf.get("spark.naga.intervalMs", "5000").toLong
    val numPools = conf.get("spark.naga.numPools", "10").toInt
    val threads = (1 to numPools).map(makeThread)
    // Slaq conf
    val slaqEnabled = conf.get("spark.slaq.enabled", "true").toBoolean
    val slaqIntervalMs = conf.get("spark.slaq.intervalMs", "5000").toLong
    val monitorIntervalMs = 5000
    // PoolReweighterLoss.start((slaqIntervalMs / 1000).toInt, fair = !slaqEnabled)
    @volatile var keepRunning = true
    val monitorThread = new Thread {
      override def run(): Unit = {
        while (keepRunning) {
          val lines = sc.getAllPools.map { p =>
            val name = p.name
            s"$name = ${sc.getPoolWeight(name)}"
          }
          println("\n\n\n########################################\n" +
            "POOL WEIGHTS:\n" +
            lines.mkString("\n") + "\n" +
            "\n########################################\n\n\n")
          Thread.sleep(monitorIntervalMs)
        }
      }
    }
    monitorThread.start()
    threads.foreach { t =>
      t.start()
      Thread.sleep(intervalMs)
    }
    threads.foreach(_.join())
    keepRunning = false
    // PoolReweighterLoss.stop()
  }

  def setup(sqlContext: SQLContext): Unit = {
    val conf = sqlContext.sparkContext.getConf
    val inputPath = conf.get("spark.naga.inputPath", "data/students.json")
    val numPartitions = conf.get("spark.naga.numPartitions", "1000").toInt
    val cacheInput = conf.get("spark.naga.cacheInput", "false").toBoolean
    val streamedRelations = sqlContext.getConf(STREAMED_RELATIONS, TABLE_NAME)
    val numBatches = sqlContext.getConf(NUMBER_BATCHES, "100")
    val numBootstrapTrials = sqlContext.getConf(NUMBER_BOOTSTRAP_TRIALS, "100")
    sqlContext.setConf(STREAMED_RELATIONS, streamedRelations)
    sqlContext.setConf(NUMBER_BATCHES, numBatches)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, numBootstrapTrials)
    // Make some threads, one per streamed relation
    val df = sqlContext.read.json(inputPath)
    sqlContext
      .createDataFrame(df.rdd.repartition(numPartitions), df.schema)
      .registerTempTable(TABLE_NAME)
    if (cacheInput) {
      sqlContext.cacheTable(TABLE_NAME)
      sqlContext.table(TABLE_NAME).count() // materialize
    }
  }

  def makeOnlineDF(sqlContext: SQLContext): OnlineDataFrame = {
    val conf = sqlContext.sparkContext.getConf
    val query = conf.get("spark.naga.query", s"SELECT AVG(uniform) FROM $TABLE_NAME")
    sqlContext.sql(query).online
  }

  private def makeThread(poolNum: Int): Thread = {
    new Thread {
      private val poolName = "pool" + poolNum
      private var odf: OnlineDataFrame = _
      private var outputPath: String = _

      setup()

      private def setup(): Unit = {
        val sc = SparkContext.getOrCreate()
        val sqlContext = SQLContext.getOrCreate(sc)
        val slaqEnabled = sc.conf.get("spark.slaq.enabled", "true").toBoolean
        sc.setLocalProperty("spark.scheduler.pool", poolName)
        sc.addSchedulablePool(poolName, 0, 10 * 1000 * 1000)
        // If this is fair share, well, do fair share!
        if (!slaqEnabled) {
          sc.getAllPools.foreach { p =>
            sc.setPoolWeight(p.name, 1000)
          }
        }
        // Some confs
        val conf = sc.getConf
        val outputDir = conf.get("spark.naga.outputDir", ".")
        val outputSuffix = conf.get("spark.naga.outputSuffix", "")
        val prepareDFs = conf.getBoolean("spark.naga.prepareDataFrames", false)
        val _outputPath =
          if (outputSuffix.nonEmpty) {
            s"$outputDir/$poolName.$outputSuffix.dat"
          } else {
            s"$outputDir/$poolName.dat"
          }
        ensureDirExists(outputDir)
        // PoolReweighterLoss.register(poolName)
        // Run IOLAP
        val _odf = makeOnlineDF(sqlContext)
        _odf.hasNext // DO NOT REMOVE THIS LINE!
        if (prepareDFs) {
          _odf.prepareDataFrames()
        }
        odf = _odf
        outputPath = _outputPath
      }

      override def run(): Unit = {
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
