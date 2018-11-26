
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

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._


/**
  * Created by logan on 7/9/2018.
  */
object TPCHQ17 extends Logging {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext
    val sqlContext = new HiveContext(sc)

    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, "2")
    sqlContext.setConf(NUMBER_BATCHES, "80")
    sqlContext.setConf(STREAMED_RELATIONS, "lineitem")

    // sqlContext.createExternalTable("lineitem", "/logantmp/tpch/dbgen/s1/lineitem.parquet", "parquet")
    // sqlContext.createExternalTable("part", "/logantmp/tpch/dbgen/s1/part.parquet", "parquet")
    println("====================1=====================")

    val df = sqlContext.read.format("parquet").load("/logantmp/tpch/dbgen/s1_2/lineitem.parquet")
    val df2 = sqlContext.createDataFrame(df.rdd, df.schema)
    df2.registerTempTable("lineitem")
    val df3 = sqlContext.read.format("parquet").load("/logantmp/tpch/dbgen/s1_2/part.parquet")
    val df4 = sqlContext.createDataFrame(df3.rdd, df3.schema)
    df4.registerTempTable("part")

    println("====================2======================")

    
val odf = sqlContext.sql("""
SELECT
    SUM(l_extendedprice) / 7.0 as avg_yearly
FROM
    lineitem,
    part as opart,
    (SELECT
        ipart.p_mfgr,
        (0.2 * AVG(l_quantity)) as quant
    FROM
        lineitem, part as ipart
    WHERE
        l_partkey = ipart.p_partkey
    group by
        ipart.p_mfgr) q
WHERE
    p_partkey = l_partkey AND
    p_brand = 'Brand#23' AND
    p_container = 'MED BOX' AND
    opart.p_mfgr = q.p_mfgr AND
    l_quantity < q.quant""").online

    odf.prepareDataFrames()
    val x = (1 to odf.progress._2).map(i => odf.collectNext())
    x.foreach{
        i => println(i)
    }
    // println(odf.collect())
  }

}

