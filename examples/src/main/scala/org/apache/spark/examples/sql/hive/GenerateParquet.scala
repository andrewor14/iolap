
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkContext}

object GenerateParquet extends Logging {

  // combine jsons

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val sqlContext = new HiveContext(sc)
    val outputFile = sc.getConf.get("spark.approx.outputFile",
      "/disk/local/disk2/stafman/generate_16k.parquet")

    // val originalDF = sqlContext.read.parquet(inputFile)
    // originalDF.unionAll(originalDF).write.parquet(outputFile)
    val dfs = new Array[DataFrame](10)
    (1 to 6).foreach {x =>
      val inputName = s"/logantmp/tmp$x.json"
      dfs(x-1) = sqlContext.read.json(inputName)
    }
    val df = dfs(0).unionAll(dfs(1)).unionAll(dfs(2))
      .unionAll(dfs(3)).unionAll(dfs(4)).unionAll(dfs(5))

    val newDF = sqlContext.createDataFrame(df.rdd.repartition(16000), df.schema)
    newDF.write.parquet(outputFile)
  }
}

