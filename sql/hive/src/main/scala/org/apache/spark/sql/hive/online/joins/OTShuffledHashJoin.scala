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

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.hive.online.ComposeRDDFunctions._
import org.apache.spark.sql.hive.online._
import org.apache.spark.sql.metric.SQLMetrics
import org.apache.spark.storage.{OLABlockId, StorageLevel}

case class OTShuffledHashJoin(
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

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  def retrieveState(): RDD[HashedRelation] = prevBatch match {
    case Some(bId) =>
      val numParts = controller.olaBlocks(opId, bId)
      OLABlockRDD.create[HashedRelation](sparkContext, opId.id, Array((numParts, bId)), numParts)
    case None =>
      sys.error(s"Unexpected prevBatch = $prevBatch")
  }

  override def doExecute(): RDD[Row] = {
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }
    val numOutputRows = longMetric("numOutputRows")
    prevBatch match {
      case None =>
        val buildRdd = buildPlan.execute()
        controller.olaBlocks((opId, currentBatch)) = buildRdd.partitions.length

        buildRdd.zipPartitionsWithIndex(streamedPlan.execute()) { (index, buildIter, streamIter) =>
          val hashed = HashedRelation(buildIter, numBuildRows, buildSideKeyGenerator)
          SparkEnv.get.blockManager.putSingle(
            OLABlockId(opId.id, currentBatch, index), hashed,
            IolapUtils.checkStorageLevel(StorageLevel.MEMORY_AND_DISK))
          hashJoin(streamIter, numStreamedRows, hashed, numOutputRows)
        }
      case Some(_) =>
        retrieveState().zipPartitionsWithIndex(streamedPlan.execute()) {
          (index, buildIter, streamIter) =>
            val hashed = buildIter.next()
            hashJoin(streamIter, numStreamedRows, hashed, numOutputRows)
        }
    }
  }

  override protected final def otherCopyArgs: Seq[AnyRef] = controller :: trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"

  override def newBatch(newTrace: List[Int]): SparkPlan =
    OTShuffledHashJoin(leftKeys, rightKeys, buildSide, left, right)(controller, newTrace, opId)
}
