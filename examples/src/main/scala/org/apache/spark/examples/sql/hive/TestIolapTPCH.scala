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
  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = new HiveContext(sc)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, "80")
    sqlContext.setConf(NUMBER_BATCHES, "80")
    sqlContext.setConf(STREAMED_RELATIONS, "lineitem")
    val df = sqlContext.read.format("parquet").load("/home/haoyuz/iolap/data/s1/lineitem.parquet")
    val df2 = sqlContext.createDataFrame(df.rdd, df.schema)
    df2.registerTempTable("lineitem")

    val odf = sqlContext.sql("""select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-01-02'
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus""").online
    odf.hasNext
    val x = (1 to odf.progress._2).map(i => odf.collectNext())
    println(x)
    }
}
