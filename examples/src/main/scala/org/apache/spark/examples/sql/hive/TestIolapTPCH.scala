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

object TestIolapTPCH extends Logging {
  def makeThread(index: Int, sqlContext: HiveContext, tblIndex: Int): Thread = {
    new Thread {
      private val name = s"t$index"
      private val tableName = s"table$tblIndex"
      private val sc = sqlContext.sparkContext
      private val avgColumn = sc.getConf.get("spark.approx.avgColumn", "normal")
      private val logDir = sc.getConf.get("spark.approx.logDir",
        "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
      private val odf = sqlContext
        .sql(s"SELECT p_brand,p_type,p_size,count(distinct ps_suppkey) as supplier_cnt from partsupp,part where p_partkey = ps_partkey and p_brand <> 'Brand#45' and p_type not like 'MEDIUM POLISHED%' and p_size in (49, 14, 23, 45, 19, 3, 36, 9) and ps_suppkey not in (select s_suppkey from supplier where s_comment like '%Customer%Complaints%') group by p_brand, p_type, p_size order by supplier_cnt desc, p_brand, p_type, p_size").online
      odf.prepareDataFrames()

      override def run(): Unit = {
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        var prevLoss = 0.0
        var initialDLoss = 0.0
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time ${System.currentTimeMillis() - t1}")
          var lossSum = 0.0
          var currentResult = ""
          col.foreach { c =>
            logInfo(s"LOGAN: DEBUG: ${c.get(0)}")
            val avg = c.get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(0)
            val low = c.get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(1)
            val high = c.get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(2)
            currentResult += "[" + low + "," + avg + "," + high + "]\n"
            val loss = high - low
            logInfo(s"LOGAN: avg $avg")
            lossSum += loss
          }
          val bw = new BufferedWriter(new FileWriter(s"$logDir/$name.output"))
          bw.write(currentResult.trim())
          bw.close()
          val loss = lossSum / col.length
          var dLoss =
            if (initialDLoss == 0.0) {
              initialDLoss = loss
              1.0
            } else {
              (prevLoss - loss) / initialDLoss // normalize
            }
          prevLoss = loss
          dLoss = Math.max(0, dLoss)
          val isFair = sc.getConf.get("spark.approx.isFair", "false").toBoolean
          if (!isFair) {
            sc.setPoolWeight(name, Math.max((dLoss * 10000).toInt, 1))
          } else {
            sc.setPoolWeight(name, 1)
          }
          logInfo(s"LOGAN: normalized delta loss $name $loss")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = new HiveContext(sc)
    val inputFiles = sc.getConf.get("spark.approx.inputFiles",
      "/disk/local/disk2/stafman/students30g_16k.parquet").split(",")
    val numPools = sc.getConf.get("spark.approx.numPools", "4").toInt
    val numBatches = sc.getConf.get("spark.approx.numBatches", "40")
    val numPartitions = sc.getConf.get("spark.approx.numPartitions", "16000").toInt
    val numBootstrapTrials = sc.getConf.get("spark.approx.numBootstrapTrials", "300")
    val shouldCacheTables = sc.getConf.get("spark.approx.shouldCacheTables", "true").toBoolean
    val waitPeriod = sc.getConf.get("spark.approx.waitPeriod", "20000").toInt // ms
    val streamedRelations = (0 until inputFiles.length).map { x => s"table$x" }.mkString(",")
    sqlContext.setConf(STREAMED_RELATIONS, streamedRelations)
    sqlContext.setConf(NUMBER_BATCHES, numBatches)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, numBootstrapTrials)

    // read each input file into a table and cache the table
    (0 until inputFiles.length).foreach { x =>
      val df = sqlContext.read.format("parquet").load(inputFiles(x))
      if (df.rdd.partitions.length == numPartitions) {
        val newDF = sqlContext.createDataFrame(df.rdd, df.schema)
        newDF.registerTempTable("table" + x)
      } else {
        val newDF = sqlContext.createDataFrame(df.rdd.repartition(numPartitions), df.schema)
        newDF.registerTempTable("table" + x)
      }
      // trigger the cache
      if (shouldCacheTables) {
        sqlContext.cacheTable("table" + x)
        sqlContext.sql(s"SELECT COUNT(*) FROM table$x").collect()
      }
    }

    val threads = (0 until numPools).map { i => makeThread(i, sqlContext, i % inputFiles.length) }
    threads.foreach { t =>
      t.start()
      Thread.sleep(waitPeriod)
    }
    threads.foreach { t => t.join() }
  }
}
