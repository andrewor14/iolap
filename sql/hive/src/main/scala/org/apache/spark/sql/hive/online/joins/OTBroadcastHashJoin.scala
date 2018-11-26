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

package org.apache.spark.sql.hive.online.joins

import java.io._

import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Expression, Row}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.hive.online.{OTStateful, OnlineDataFrame, OpId}
import org.apache.spark.sql.metric.SQLMetrics
import org.apache.spark.sql.types.UTF8String

import scala.concurrent._
import scala.concurrent.duration._

/**
 * One-time broadcast hash join.
 */
case class OTBroadcastHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)(
    @transient val controller: OnlineDataFrame,
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
  extends BinaryNode with HashJoin with OTStateful {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  val timeout = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  @transient
  private lazy val broadcastFuture: Future[Broadcast[Array[Byte]]] = future {
    prevBatch match {
      case None =>
        val compressionCodec = CompressionCodec.createCodec(SparkEnv.get.conf)
        val baos = new ByteArrayOutputStream()
        val os = compressionCodec.compressedOutputStream(baos)
        val input = buildPlan.execute().map(_.copy())
        // first write schema to baos
        val schema = input.take(1).toSeq.map(x => x.getClass.getCanonicalName).mkString("*") + "\n"
        os.write(schema.getBytes())
        input.toLocalIterator.foreach(row => {
          // convert row to bytes
          val string = row.toSeq.mkString("*") + "\n"
          os.write(string.getBytes())
        })
        os.close()
        // val hashed = HashedRelation(
        //   input.toLocalIterator, SQLMetrics.nullLongMetric, buildSideKeyGenerator, 1000 * 1000)
        val broadcast = sparkContext.broadcast(baos.toByteArray())
        controller.broadcasts((opId, currentBatch)) = broadcast
        broadcast
      case Some(bId) =>
        controller.broadcasts((opId, bId)).asInstanceOf[Broadcast[Array[Byte]]]
    }
  }(BroadcastHashJoin.broadcastHashJoinExecutionContext)

  override def doExecute(): RDD[Row] = {
    val numStreamedRows = buildSide match {
      case BuildLeft => longMetric("numRightRows")
      case BuildRight => longMetric("numLeftRows")
    }
    val numOutputRows = longMetric("numOutputRows")
    // val broadcastRelation = broadcastFuture
    // val broadcastRelation = Await.result(broadcastFuture, timeout)
    val broadcastBytes = Await.result(broadcastFuture, timeout)
    streamedPlan.execute().mapPartitions { streamedIter =>
      // decompress
      val compressionCodec = CompressionCodec.createCodec(SparkEnv.get.conf)
      val bais = new ByteArrayInputStream(broadcastBytes.value)
      val cstream = new BufferedInputStream(compressionCodec.compressedInputStream(bais))
      // first read the first row to get the schema
      val reader = new BufferedReader(new InputStreamReader(cstream))
      val schemaStr = reader.readLine().split("\\*")
      val iterator = new Iterator[Row]() {
        var line = reader.readLine()
        override def hasNext: Boolean = {
          line == null
        }

        override def next: Row = {
          val fields = line.split("\\*")
          val priority = fields(0)
          val priorityUTF = new UTF8String()
          priorityUTF.set(priority.getBytes())
          val id = Integer.parseInt(fields(1))
          line = reader.readLine()
          Row.fromSeq(Seq(priority, id))
        }
      }

      // broadcastRelation.value
      val hashedRelation = HashedRelation(iterator, SQLMetrics.nullLongMetric, buildSideKeyGenerator, iterator.length)
      hashJoin(streamedIter, numStreamedRows, hashedRelation, numOutputRows)
    }
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = controller :: trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan = {

    val join = OTBroadcastHashJoin(leftKeys, rightKeys, buildSide, left, right)(
      controller, newTrace, opId)
    join.broadcastFuture
    join
  }
}
