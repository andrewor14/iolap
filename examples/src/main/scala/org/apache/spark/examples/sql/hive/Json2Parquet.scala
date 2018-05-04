
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

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}

object Json2Parquet extends Logging {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val sqlContext = new HiveContext(sc)
    val inputFiles = sc.getConf.get("spark.approx.inputFiles",
      "/disk/local/disk2/stafman/students14g.json").split(",")
    val outputFiles = sc.getConf.get("spark.approx.outputFiles",
      "/disk/local/disk2/stafman/students14g.parquet").split(",")
    logInfo(s"inputFiles...${inputFiles(0)}")
    // read each input file into a table and cache the table
    (0 until inputFiles.length).foreach { x =>
      val df = sqlContext.read.json(inputFiles(x))
      val newDF = sqlContext.createDataFrame(df.rdd.repartition(16000), df.schema)
      newDF.write.parquet(outputFiles(x))
    }
  }
}

