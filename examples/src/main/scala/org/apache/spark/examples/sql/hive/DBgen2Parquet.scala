
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

import scala.collection.immutable.HashMap

object DBgen2Parquet extends Logging {

  // combine jsons

  def main(args: Array[String]): Unit = {
    val numPartitions = 16000
    val sc = new SparkContext
    val sqlContext = new HiveContext(sc)
    val tblNames = sc.getConf.get("spark.approx.tblNames",
//      "region,supplier,customer,orders,lineitem,part,partsupp,nation").split(",")
    "orders").split(",")
    val inputOutputDir = sc.getConf.get("spark.approx.fileDir",
      "/home/stafman/iolap/tpch/dbgen/")
    val schemas = HashMap(("lineitem", Seq("L_ORDERKEY", "L_PARTKEY", "L_SUPPKEY",
                                          "L_LINENUMBER", "L_QUANTITY", "L_EXTENDEDPRICE",
                                          "L_DISCOUNT", "L_TAX",
                                          "L_RETURNFLAG", "L_LINESTATUS", "L_SHIPDATE",
                                          "L_COMMITDATE", "L_RECEIPTDATE",
                                          "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT")),
                          ("orders", Seq("O_ORDERKEY", "O_CUSTKEY", "O_ORDERSTATUS",
                                          "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY",
                                          "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT")),
                          ("customer", Seq("C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_NATIONKEY",
                                            "C_PHONE", "C_ACCTBAL", "C_MKTSEGMENT", "C_COMMENT")),
                          ("partsupp", Seq("PS_PARTKEY", "PS_SUPPKEY", "PS_AVAILQTY",
                                            "PS_SUPPLYCOST", "PS_COMMENT")),
                          ("supplier", Seq("S_SUPPKEY", "S_NAME", "S_ADDRESS", "S_NATIONKEY",
                                            "S_PHONE", "S_ACCTBAL", "S_COMMENT")),
                          ("part", Seq("P_PARTKEY", "P_NAME", "P_MFGR", "P_BRAND", "P_TYPE",
                                        "P_SIZE", "P_CONTAINER", "P_RETAILPRICE", "P_COMMENT")),
                          ("region", Seq("R_REGIONKEY", "R_NAME", "R_COMMENT")),
                          ("nation", Seq("N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT")))

    tblNames.foreach { tblName =>
      val inputFile = inputOutputDir + tblName + ".tbl"
      val outputFile = inputOutputDir + tblName + ".parquet"
      val df = sqlContext.read
        .format("com.databricks.spark.csv")
        .option("head", "true")
        .option("inferSchema", "true")
        .option("delimiter", "|")
        .load(inputFile)
      val renamedDF = df.drop(df.columns.last).toDF(schemas(tblName): _*)
      val finalDF = sqlContext.createDataFrame(renamedDF.rdd.repartition(numPartitions), renamedDF.schema)
      finalDF.write.parquet(outputFile)
    }
  }
}

