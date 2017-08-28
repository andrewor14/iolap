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

import org.apache.spark.sql.Column
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._
import org.apache.spark.sql.hive.online.RandomSeed

object TestIolap extends Logging {
  def makeThread(sqlContext: HiveContext, name: String): Thread = {
    new Thread {
        val sc = sqlContext.sparkContext
        val avgColumn = sc.getConf.get("spark.slaq.avgColumn", "uniform")
        val odf = sqlContext.sql(s"SELECT AVG($avgColumn) FROM table WHERE zipf > 2 AND twin_peak > 1000").online
        odf.prepareDataFrames()

      override def run(): Unit = {
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        val result = (1 to odf.progress._2).map { i =>
          logInfo(s"LOGAN: job queued for $name")
          val col = odf.collectNext()
          val avg = col(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(0)
          val low = col(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(1)
          val high = col(0).get(0).asInstanceOf[org.apache.spark.sql.Row].getDouble(2)
          val loss = high - low
          logInfo(s"LOGAN: $name $avg $loss")
          val isFair = sc.getConf.get("spark.slaq.isFair", "false").equals("true")
          if (!isFair) {
            /*
            if (loss == 0) sc.setPoolWeight(name, 0)
            else sc.setPoolWeight(name, ((loss - 1.25) / (16.5-1.25) * 1000).toInt)
            */
            sc.setPoolWeight(name, loss.toInt)
          } else {
            sc.setPoolWeight(name, 1)
          }
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
    val numBatches = sc.getConf.get("spark.slaq.numBatches", "160")
    val streamedRelations = poolNames.mkString(",")
    val numBootstrapTrials = "200"
    val waitPeriod = 60000


    val numPartitions = sc.getConf.get("spark.slaq.numPartitions", "16000").toInt
    val inputFile = sc.getConf.get("spark.slaq.inputFile", "data/students1g.json")
    val df = sqlContext.read.json(inputFile)
    val newDF = sqlContext.createDataFrame(
      df.rdd.repartition(numPartitions), df.schema)
    newDF.registerTempTable("table")
    sqlContext.table("table").withColumn(SEED_COLUMN, new Column(RandomSeed()))
//     sqlContext.cacheTable("table")
    sqlContext.table("table").count()

    sqlContext.setConf(STREAMED_RELATIONS, "table")
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
