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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.hive.online.OnlineSQLConf._
import org.apache.spark.sql.hive.online.OnlineSQLFunctions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object TestIolapPR extends Logging {




  // works for 1, 100, 300, 1000
  def makeThreadTPCHQ1(index: Int, sqlContext: HiveContext): Thread = {
    new Thread {
      /*
      val groundTruth = Array(
        Seq("A", "F", 37757113842L,
          5.6616527268757055E13,
          5.378566580027875E13,
          5.593707147105494E13,
          25.499822495917584,
          38236.80490330902,
          0.05000127879636337,
          1480681438),
        Seq("N", "F", 985802167L,
          1.4781694017276245E12,
          1.4042564644665488E12,
          1.460443979681066E12,
          25.501163950446003,
          38237.936090871706,
          0.050007796230368246,
          38657144),
        Seq("N", "O", 59138286906L,
          8.867811995288655E13,
          8.424423356333912E13,
          8.76140664389501E13,
          25.49998136407897,
          38237.333621017395,
          0.04999979346750536,
          2319150201L),
        Seq("R", "F", 37757365731L,
          5.661701527762697E13,
          5.378616300373275E13,
          5.593759933581342E13,
          25.500100042872333,
          38237.295578143654,
          0.04999960114820497,
          1480675200))
          */
      val groundTruth = Seq(37757113842.0, 985802167.0, 59138286906.0, 37757365731.0)


      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        val name = s"${index}Q1"
        val logDir = sc.getConf.get("spark.approx.logDir",
          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
        val odf = sqlContext
          .sql(
            """select
                    l_returnflag,
                    l_linestatus,
                    sum(l_quantity) as sum_qty,
                    sum(l_extendedprice) as
                    sum_base_price,
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
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time $name ${System.currentTimeMillis() - t1}")
          var lossSum = 0.0
          var currentResult = ""
//          val progress = resultToProgress(name, col, Seq(2, 3, 4, 5, 6, 7, 8, 9))
          val progress = resultToProgress(name, col, Seq(2))
          logInfo(s"LOGAN: DEBUG $name $progress")
          val distToTruth =
            resultToGroundTruthDist(name, col, 2, groundTruth)
          logInfo(s"LOGAN: DEBUG2 $name $distToTruth")
          if (sc.getConf.get("spark.approx.enabled", "false").toBoolean) {
            sc.setPoolWeight(name, (1000000 * progress).toInt)
          }
        }
      }
    }
  }



  /*
//   * Currently works for...not sure...orders is too big to hash!
//   */
//  def makeThreadTPCHQ5(index: Int, sqlContext: HiveContext): Thread = {
//    new Thread {
//      override def run(): Unit = {
//        val sc = sqlContext.sparkContext
//        val name = s"Q5"
//        val logDir = sc.getConf.get("spark.approx.logDir",
//          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
//
//        val odf = sqlContext.sql(
//          """select
//          n_name,
//          sum(l_extendedprice * (1 - l_discount)) as revenue
//      from
//          customer,
//          orders,
//          lineitem,
//          supplier,
//          nation,
//          region
//      where
//          c_custkey = o_custkey
//          and l_orderkey = o_orderkey
//          and l_suppkey = s_suppkey
//          and c_nationkey = s_nationkey
//          and s_nationkey = n_nationkey
//          and n_regionkey = r_regionkey
//          and r_name = 'ASIA'
//          and o_orderdate >= date '1994-01-01'
//          and o_orderdate < date '1995-01-01'
//      group by
//          n_name""").online
//        odf.hasNext
//        sc.setLocalProperty("spark.scheduler.pool", name)
//        sc.addSchedulablePool(name, 0, 1000000)
//        (1 to odf.progress._2).foreach { i =>
//          val t1 = System.currentTimeMillis()
//          val col = odf.collectNext()
//          logInfo(s"LOGAN: collectNext time $name ${System.currentTimeMillis() - t1}")
//          col.foreach { c =>
//            logInfo(s"LOGAN: DEBUG $name: ${c}")
//          }
//        }
//      }
//    }
//  }

  // works for 1, 100, 300, 1000
  def makeThreadTPCHQ6(index: Int, sqlContext: HiveContext): Thread = {

    val groundTruth = Seq(7.535403260988173E10)

    new Thread {
      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        val name = s"${index}Q6"
        val logDir = sc.getConf.get("spark.approx.logDir",
          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")

        val odf = sqlContext.sql(
          """
            select
                sum(l_extendedprice * l_discount) as revenue
            from
                lineitem
            where
                l_shipdate >= date '1994-01-01'
                and l_shipdate < date '1995-01-01'
                and l_discount > 0.06 - 0.01
                and l_discount < 0.06 + 0.01
                and l_quantity < 24
          """.stripMargin).online
        odf.hasNext
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time ${System.currentTimeMillis() - t1}")
          val progress = resultToProgress(name, col, Seq(0))
          logInfo(s"LOGAN: DEBUG $name $progress")
          val distToTruth =
            resultToGroundTruthDist(name, col, 0, groundTruth)
          logInfo(s"LOGAN: DEBUG2 $name $distToTruth")
          if (sc.getConf.get("spark.approx.enabled", "false").toBoolean) {
            sc.setPoolWeight(name, (1000000 * progress).toInt)
          }
        }
      }
    }
  }

  /*
//   * Currently works for s1,s30...orders is too big to hash!
//   */
//  def makeThreadTPCHQ12(index: Int, sqlContext: HiveContext): Thread = {
//    new Thread {
//      override def run(): Unit = {
//        val sc = sqlContext.sparkContext
//        val name = s"Q12"
//        val logDir = sc.getConf.get("spark.approx.logDir",
//          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
//
//        val odf = sqlContext.sql(
//          """
//            select
//                l_shipmode,
//                sum(case
//                    when o_orderpriority = '1-URGENT'
//                        or o_orderpriority = '2-HIGH'
//                        then 1
//                    else 0
//                end) as high_line_count,
//                sum(case
//                    when o_orderpriority <> '1-URGENT'
//                        and o_orderpriority <> '2-HIGH'
//                        then 1
//                    else 0
//                end) as low_line_count
//            from
//                orders,
//                lineitem
//            where
//                o_orderkey = l_orderkey
//                and l_shipmode in ('MAIL', 'SHIP')
//                and l_commitdate < l_receiptdate
//                and l_shipdate < l_commitdate
//                and l_receiptdate >= date '1994-01-01'
//                and l_receiptdate < date '1995-01-01'
//            group by
//                l_shipmode
//            order by
//                l_shipmode
//          """).online
//        odf.hasNext
//        sc.setLocalProperty("spark.scheduler.pool", name)
//        sc.addSchedulablePool(name, 0, 1000000)
//
//        (1 to odf.progress._2).foreach { i =>
//          val t1 = System.currentTimeMillis()
//          val col = odf.collectNext()
//          logInfo(s"LOGAN: collectNext time ${System.currentTimeMillis() - t1}")
//          col.foreach { c =>
//            logInfo(s"LOGAN: DEBUG $name: ${c}")
//          }
//        }
//
//        // val x = odf.collect()
//        // x.foreach { c => logInfo(s"LOGAN: DEBUG: $c") }
//      }
//    }
//  }

  /*
   * Works for 1, 100
   */
  def makeThreadTPCHQ14(index: Int, sqlContext: HiveContext): Thread = {
    new Thread {
      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        val name = s"Q14"
        val logDir = sc.getConf.get("spark.approx.logDir",
          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")


        val odf = sqlContext.sql("""
            select
                100.00 * sum(case
                    when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else 0
                end ) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
            from
                lineitem100,
                part100
            where
                l_partkey = p_partkey
                and l_shipdate >= '1996-12-01'
                and l_shipdate < '1997-01-01'
        """).online
        odf.hasNext
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time $name ${System.currentTimeMillis() - t1}")
          val progress = resultToProgress(name, col, Seq(0))
          logInfo(s"LOGAN: DEBUG $name $progress")
          if (sc.getConf.get("spark.approx.enabled", "false").toBoolean) {
            sc.setPoolWeight(name, (1000000 * progress).toInt)
          }
        }
      }
    }
  }


  // works on s1, s100, s1000 (make sure partsupp is a streamed relation!)
  def makeThreadTPCHQ16(index: Int, sqlContext: HiveContext): Thread = {
    new Thread {
      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        val name = s"${index}Q16"
        val logDir = sc.getConf.get("spark.approx.logDir",
          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
        import scala.io.Source
        val groundTruth = Source.fromFile("/home/haoyuz/q16groundtruth.data")
          .getLines.toSeq.map { x => x.toDouble }

        val innerTbl = sqlContext
          .sql("SELECT s_suppkey FROM supplier WHERE s_comment like '%Customer%Complaints%'")
          .collect()
        val innerTblStr = innerTbl.map(x => x(0)).mkString("(", ", ", ")")

        val odf = sqlContext.sql(
          s"""
          select
              p_brand,
              p_type,
              p_size,
              count(ps_suppkey) as supplier_cnt
          from
              partsupp,
              part
          where
              p_partkey = ps_partkey
              and p_brand <> 'Brand#43'
              and p_type not like 'STANDARD BURNISHED%'
              and p_size in (22, 7, 8, 35, 33, 11, 31, 39)
              and ps_suppkey not in ${innerTblStr}
          group by
              p_brand,
              p_type,
              p_size
          """).online
        odf.hasNext
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time ${System.currentTimeMillis() - t1}")
          val progress = resultToProgress(name, col, Seq(3))
          logInfo(s"LOGAN: DEBUG $name $progress")
          val distToTruth =
            resultToGroundTruthDist(name, col, 3, groundTruth)
          logInfo(s"LOGAN: DEBUG2 $name $distToTruth")
          if (sc.getConf.get("spark.approx.enabled", "false").toBoolean) {
            sc.setPoolWeight(name, (1000000 * progress).toInt)
          }
        }
      }
    }
  }


  /*
   * Currently works for s1, s100, s1000 (...but something weird is going on...)
   */
  def makeThreadTPCHQ17(index: Int, sqlContext: HiveContext): Thread = {
    new Thread {
      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        val name = s"Q17"
        val logDir = sc.getConf.get("spark.approx.logDir",
          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")

        val odf = sqlContext.sql(
          """
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
                l_quantity < q.quant
          """).online
        odf.hasNext
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        logInfo(s"LOGAN: numBatches: ${odf.progress._2}")
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time $name ${System.currentTimeMillis() - t1}")
          val progress = resultToProgress(name, col, Seq(0))
          logInfo(s"LOGAN: DEBUG $name $progress")
          if (sc.getConf.get("spark.approx.enabled", "false").toBoolean) {
            sc.setPoolWeight(name, (1000000 * progress).toInt)
          }
        }
      }
    }
  }

  // works on s1, s100, s1000
  def makeThreadTPCHQ19(index: Int, sqlContext: HiveContext): Thread = {
    new Thread {

      val groundTruth = Seq(3.9746783091858068E9)

      override def run(): Unit = {
        val sc = sqlContext.sparkContext
        val name = s"${index}Q19"
        val logDir = sc.getConf.get("spark.approx.logDir",
          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")

        val odf = sqlContext.sql(
          """
          SELECT
            sum(l_extendedprice * (1 - l_discount)) as revenue
          FROM
            lineitem,
            part
          WHERE
            ((
                p_partkey = l_partkey
                AND p_brand = 'Brand#45'
                AND p_container in ('SM_CASE', 'SM_BOX', 'SM_PACK', 'SM_PKG')
                AND l_quantity >= 7
                AND l_quantity <= 17
                AND p_size >= 1 AND p_size <= 5
                AND l_shipmode in ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            )
            OR
            (
                p_partkey = l_partkey
                AND p_brand = 'Brand#51'
                AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                AND l_quantity >= 20 AND l_quantity <= 30
                AND p_size >= 1 AND p_size <= 10
                AND l_shipmode in ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            )
            OR
            (
                p_partkey = l_partkey
                AND p_brand = 'Brand#51'
                AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                AND l_quantity >= 28 AND l_quantity <= 38
                AND p_size >= 1 AND p_size <= 15
                AND l_shipmode in ('AIR', 'AIR REG')
                AND l_shipinstruct = 'DELIVER IN PERSON'
            ))
          """.stripMargin).online
        odf.hasNext
        sc.setLocalProperty("spark.scheduler.pool", name)
        sc.addSchedulablePool(name, 0, 1000000)
        (1 to odf.progress._2).foreach { i =>
          val t1 = System.currentTimeMillis()
          val col = odf.collectNext()
          logInfo(s"LOGAN: collectNext time ${System.currentTimeMillis() - t1}")
          val progress = resultToProgress(name, col, Seq(0))
          logInfo(s"LOGAN: DEBUG $name $progress")
          val distToTruth =
            resultToGroundTruthDist(name, col, 0, groundTruth)
          logInfo(s"LOGAN: DEBUG2 $name $distToTruth")
          if (sc.getConf.get("spark.approx.enabled", "false").toBoolean) {
            sc.setPoolWeight(name, (1000000 * progress).toInt)
          }
        }
      }
    }
  }



  // works for ???
//  def makeThreadTPCHQ22(index: Int, sqlContext: HiveContext): Thread = {
//    new Thread {
//      override def run(): Unit = {
//        val sc = sqlContext.sparkContext
//        val name = s"Q22"
//        val logDir = sc.getConf.get("spark.approx.logDir",
//          "/disk/local/disk1/stafman/iolap-princeton/dashboard/")
//
//        val odf = sqlContext.sql(
//          """
//          select
//              cntrycode,
//              count(*) as numcust,
//              sum(c_acctbal) as totacctbal
//          from (
//              select
//                  substring(c_phone, 1, 2) as cntrycode,
//                  c_acctbal
//              from
//                  customer
//              where
//                  substring(c_phone, 1, 2) in ('26', '29', '31', '28', '30', '25', '40')
//                  and c_acctbal > (
//                      select
//                          avg(c_acctbal)
//                      from
//                          customer
//                      where
//                          c_acctbal > 0.00
//                          and substring(c_phone, 1, 2)
  // in ('26', '29', '31', '28', '30', '25', '40')
//                  )
//                  and not exists (
//                      select
//                          *
//                      from
//                          orders
//                      where
//                          o_custkey = c_custkey
//                  )
//              ) as custsale
//          group by
//              cntrycode
//          order by
//              cntrycode
//          """.stripMargin).online
//        odf.hasNext
//        sc.setLocalProperty("spark.scheduler.pool", name)
//        sc.addSchedulablePool(name, 0, 1000000)
//        (1 to odf.progress._2).foreach { i =>
//          val t1 = System.currentTimeMillis()
//          val col = odf.collectNext()
//          logInfo(s"LOGAN: collectNext time ${System.currentTimeMillis() - t1}")
//          col.foreach { c =>
//            logInfo(s"LOGAN: DEBUG $name: ${c}")
//          }
//        }
//      }
//    }
//  }

  val lossHistory: mutable.HashMap[String, ArrayBuffer[Seq[Double]]] = mutable.HashMap()
  // val lossDiffHistory: mutable.HashMap[String, ArrayBuffer[Seq[Double]]] = mutable.HashMap()
  // NS stands for normalized and smoothed
  // val lossDiffHistoryNS: mutable.HashMap[String, ArrayBuffer[Double]] = mutable.HashMap()

  val resultHistory: mutable.HashMap[String, ArrayBuffer[Array[Row]]] = mutable.HashMap()

  // assume only one approxCol for now....
  def resultToGroundTruthDist(name: String, result: Array[Row], approxCol: Int,
                              groundTruth: Seq[Double]): Double = {
    if (!resultHistory.contains(name)) {
      resultHistory.put(name, ArrayBuffer[Array[Row]]())
    }
    val poolResultHistory: ArrayBuffer[Array[Row]] =
      resultHistory.getOrElse(name, ArrayBuffer[Array[Row]]())
    poolResultHistory.append(result)
    val errorsHistory = poolResultHistory.map { r =>
      val estimations = r.map { row =>
        val estimation = row.toSeq(approxCol).asInstanceOf[GenericRowWithSchema]
        estimation(0).toString.toDouble
      }
      val diffs = estimations.zip(groundTruth).map { case (x: Double, y: Double) =>
          Math.abs(x - y)
      }
      diffs.foreach { x => logInfo(s"LOGAN: C: $x")}
      diffs
    }
    // for each
    val relativeErrors = errorsHistory.last.zip(errorsHistory.head).map { case(x: Double, y: Double) =>
        x / y
    }
    logInfo(s"LOGAN testing ABC ${relativeErrors.sum} ${relativeErrors.size} ${relativeErrors.length}")
    relativeErrors.sum / relativeErrors.size
  }

  def resultToProgress(name: String, result: Array[Row], approxCols: Seq[Int]): Double = {
    var retVal = 1.0
    val N: Int = 3 // smoothing factor


    if(approxCols.length == 1) {

      if (!lossHistory.contains(name)) {
        lossHistory.put(name, ArrayBuffer[Seq[Double]]())
      }
      val poolLossHist: ArrayBuffer[Seq[Double]] =
        lossHistory.getOrElse(name, ArrayBuffer[Seq[Double]]())

      // if we have only one approx col
      if (result.length == 1) {
        // if we only have one row in the results
        val estAndBounds = result(0).toSeq(approxCols(0)).asInstanceOf[GenericRowWithSchema]
        val estimation = estAndBounds(0).toString.toDouble
        poolLossHist.append(Seq(estimation))
        if(poolLossHist.size > 1) {
          val poolLossDiffHist = (1 until poolLossHist.size).map { i =>
            Math.abs(poolLossHist(i)(0) - poolLossHist(i - 1)(0))
          }
          val max = poolLossDiffHist.max
          val poolLossDiffNormalized = poolLossDiffHist.map( x => x / max)

          val lastN = poolLossDiffNormalized.slice(poolLossDiffNormalized.size -
            Math.min(N, poolLossDiffNormalized.size), poolLossDiffNormalized.size)
          val avg = lastN.sum / lastN.size
          retVal = avg
        }
      } else if (!result.isEmpty) {
        // if not, average all of the groups
        val estimations = result.map { row =>
          val estimation = row.toSeq(approxCols(0)).asInstanceOf[GenericRowWithSchema]
          estimation(0).toString.toDouble
        }
        // estimations is a sequence of estimations for each group
        poolLossHist.append(estimations)
        if(poolLossHist.size > 1) {
          // poolLossDiffHist is a sequence of differences for each group
          val poolLossDiffHist = (1 until poolLossHist.size).map { i =>
            poolLossHist(i).zip(poolLossHist(i-1)).map { case (x: Double, y: Double) =>
                Math.abs(x-y)
            }
          }
          // maxs is the max diff for each group
          val maxs = (0 until poolLossDiffHist(0).size).map { i =>
            poolLossDiffHist.map( row => row(i)).max
          }
          // poolLossDiffNormalized is the normalized diff for each group
          val poolLossDiffNormalized = poolLossDiffHist.map { hist =>
            hist.zip(maxs).map { case(x: Double, max: Double) =>
              x / max
            }
          }
          // lastN is the last N historical points, each of which has the normalized diff
          val lastN = poolLossDiffNormalized.slice(poolLossDiffNormalized.size -
            Math.min(N, poolLossDiffNormalized.size), poolLossDiffNormalized.size)
          val avgs = lastN.map { hist =>
            hist.sum / hist.size
          }
          retVal = avgs.sum / avgs.size
        }
      }
    } else {
      // if mult approx cols, just average them?
      if (result.length == 1) {
        /* retVal = approxCols.map(col =>
          approxCols(col).asInstanceOf[Double]
        ).sum / approxCols.length
        */
      } else if (!result.isEmpty) {
        throw new UnsupportedOperationException("Don't know what to do here yet")
      }
    }
    if (retVal.isNaN) retVal = 1.0
    retVal
  }


    /*

    currently working: Q1, Q6, Q16, Q19,
     */


  def main(args: Array[String]): Unit = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = new HiveContext(sc)
    val runtime = Runtime.getRuntime
    logInfo(s"LOGAN sanity check: ${runtime.freeMemory()}")
    val largeInputNames = sc.getConf.get("spark.approx.tableNames",
      "lineitem,part,partsupp,supplier,customer,orders").split(",")
    val smallInputNames = sc.getConf.get("spark.approx.smallTableNames",
      "lineitem,part").split(",")
    val fileSize = sc.getConf.get("spark.approx.fileSize", "1")
    val smallFileSize = sc.getConf.get("spark.approx.smallFileSize", "1")
    val largeInputFiles =
      largeInputNames.map( x => s"/home/haoyuz/iolap/data/s$fileSize/$x.parquet")
    val smallInputFiles =
      smallInputNames.map( x => s"/home/haoyuz/iolap/data/s$smallFileSize/$x.parquet")
    val numPools = sc.getConf.get("spark.approx.numPools", "1").toInt
    val numBatches = sc.getConf.get("spark.approx.numBatches", "80")
    val numBootstrapTrials = sc.getConf.get("spark.approx.numBootstrapTrials", "80")
    val shouldCacheTables = sc.getConf.get("spark.approx.shouldCacheTables", "true").toBoolean
    val waitPeriod = sc.getConf.get("spark.approx.waitPeriod", "20000").toInt // ms
    val streamedRelations = "lineitem,partsupp"
    val queries = sc.getConf.get("spark.approx.queries", "Q1").split(",")
    val smallQueries = sc.getConf.get("spark.approx.smallQueries", "Q14").split(",")


    sqlContext.setConf(STREAMED_RELATIONS, streamedRelations)
    sqlContext.setConf(NUMBER_BATCHES, numBatches)
    sqlContext.setConf(NUMBER_BOOTSTRAP_TRIALS, numBootstrapTrials)

    // read each input file into a table and cache the table
    (0 until largeInputFiles.length).foreach { x =>
      val df = sqlContext.read.format("parquet").load(largeInputFiles(x))
      val newDF = sqlContext.createDataFrame(df.rdd, df.schema)
      newDF.registerTempTable(largeInputNames(x))
      // trigger the cache
      if (shouldCacheTables) {
        sqlContext.cacheTable(largeInputNames(x))
        sqlContext.sql(s"SELECT COUNT(*) FROM ${largeInputNames(x)}").collect()
      }
    }
//    if(smallFileSize != -1) {
//      (0 until smallInputFiles.length).foreach { x =>
//        val df = sqlContext.read.format("parquet").load(smallInputFiles(x))
//        val newDF = sqlContext.createDataFrame(df.rdd, df.schema)
//        newDF.registerTempTable(smallInputNames(x) + smallFileSize)
//        if (shouldCacheTables) {
//          sqlContext.cacheTable(smallInputNames(x) + smallFileSize)
//          sqlContext.sql(s"SELECT COUNT(*) FROM ${smallInputNames(x)}$smallFileSize").collect()
//        }
//      }
//    }

    var threads = (0 until numPools).flatMap { i =>
      queries.map { q =>
        q match {
          case "Q1" => makeThreadTPCHQ1(i, sqlContext)
          case "Q6" => makeThreadTPCHQ6(i, sqlContext)
          case "Q14" => makeThreadTPCHQ14(i, sqlContext)
          case "Q16" => makeThreadTPCHQ16(i, sqlContext)
          case "Q17" => makeThreadTPCHQ17(i, sqlContext)
          case "Q19" => makeThreadTPCHQ19(i, sqlContext)
        }
      }
    }
    logInfo(s"LOGAN: DEBUG: We have ${threads.size} threads")

//    threads ++= (0 until numPools).flatMap { i =>
//      smallQueries.map { q =>
//        q match {
//          case "Q1" => makeThreadTPCHQ1(i, sqlContext)
//          case "Q6" => makeThreadTPCHQ6(i, sqlContext)
//          case "Q14" => makeThreadTPCHQ14(i, sqlContext)
//          case "Q16" => makeThreadTPCHQ16(i, sqlContext)
//          case "Q17" => makeThreadTPCHQ17(i, sqlContext)
//          case "Q19" => makeThreadTPCHQ19(i, sqlContext)
//        }
//      }
//    }


    threads.foreach { t =>
      t.start()
      Thread.sleep(waitPeriod)
    }
    threads.foreach { t => t.join() }
  }
}


