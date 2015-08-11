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

package org.apache.spark.sql.execution.joins

import java.util.{HashMap => JavaHashMap}

import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.metric.{LongSQLMetric, SQLMetrics}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs a hash based outer join for two child relations by shuffling the data using
 * the join keys. This operator requires loading the associated partition in both side into memory.
 */
@DeveloperApi
case class HashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case x => throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[ClusteredDistribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def output: Seq[Attribute] = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case x =>
        throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
    }
  }

  @transient private[this] lazy val DUMMY_LIST = Seq[Row](null)
  @transient private[this] lazy val EMPTY_LIST = Seq.empty[Row]

  @transient private[this] lazy val leftNullRow = new GenericRow(left.output.length)
  @transient private[this] lazy val rightNullRow = new GenericRow(right.output.length)
  @transient private[this] lazy val boundCondition =
    condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

  // TODO we need to rewrite all of the iterators with our own implementation instead of the Scala
  // iterator for performance purpose.

  private[this] def leftOuterIterator(
      key: Row,
      joinedRow: JoinedRow,
      rightIter: Iterable[Row],
      numOutputRows: LongSQLMetric): Iterator[Row] = {
    val ret: Iterable[Row] = {
      if (!key.anyNull) {
        val temp = rightIter.collect {
          case r if boundCondition(joinedRow.withRight(r)) =>
            numOutputRows += 1
            joinedRow.copy()
        }
        if (temp.isEmpty) {
          numOutputRows += 1
          joinedRow.withRight(rightNullRow).copy :: Nil
        } else {
          temp
        }
      } else {
        numOutputRows += 1
        joinedRow.withRight(rightNullRow).copy :: Nil
      }
    }
    ret.iterator
  }

  private[this] def rightOuterIterator(
      key: Row,
      leftIter: Iterable[Row],
      joinedRow: JoinedRow,
      numOutputRows: LongSQLMetric): Iterator[Row] = {

    val ret: Iterable[Row] = {
      if (!key.anyNull) {
        val temp = leftIter.collect {
          case l if boundCondition(joinedRow.withLeft(l)) =>
            numOutputRows += 1
            joinedRow.copy()
        }
        if (temp.isEmpty) {
          numOutputRows += 1
          joinedRow.withLeft(leftNullRow).copy :: Nil
        } else {
          temp
        }
      } else {
        numOutputRows += 1
        joinedRow.withLeft(leftNullRow).copy :: Nil
      }
    }
    ret.iterator
  }

  private[this] def fullOuterIterator(
      key: Row,
      leftIter: Iterable[Row],
      rightIter: Iterable[Row],
      joinedRow: JoinedRow,
      numOutputRows: LongSQLMetric): Iterator[Row] = {
    if (!key.anyNull) {
      // Store the positions of records in right, if one of its associated row satisfy
      // the join condition.
      val rightMatchedSet = scala.collection.mutable.Set[Int]()
      leftIter.iterator.flatMap[Row] { l =>
        joinedRow.withLeft(l)
        var matched = false
        rightIter.zipWithIndex.collect {
          // 1. For those matched (satisfy the join condition) records with both sides filled,
          //    append them directly

          case (r, idx) if boundCondition(joinedRow.withRight(r)) =>
            numOutputRows += 1
            matched = true
            // if the row satisfy the join condition, add its index into the matched set
            rightMatchedSet.add(idx)
            joinedRow.copy()

        } ++ DUMMY_LIST.filter(_ => !matched).map( _ => {
          // 2. For those unmatched records in left, append additional records with empty right.

          // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
          // as we don't know whether we need to append it until finish iterating all
          // of the records in right side.
          // If we didn't get any proper row, then append a single row with empty right.
          numOutputRows += 1
          joinedRow.withRight(rightNullRow).copy()
        })
      } ++ rightIter.zipWithIndex.collect {
        // 3. For those unmatched records in right, append additional records with empty left.

        // Re-visiting the records in right, and append additional row with empty left, if its not
        // in the matched set.
        case (r, idx) if !rightMatchedSet.contains(idx) =>
          numOutputRows += 1
          joinedRow(leftNullRow, r).copy()
      }
    } else {
      leftIter.iterator.map[Row] { l =>
        numOutputRows += 1
        joinedRow(l, rightNullRow).copy()
      } ++ rightIter.iterator.map[Row] { r =>
        numOutputRows += 1
        joinedRow(leftNullRow, r).copy()
      }
    }
  }

  private[this] def buildHashTable(
      iter: Iterator[Row],
      numIterRows: LongSQLMetric,
      keyGenerator: Projection): JavaHashMap[Row, CompactBuffer[Row]] = {
    val hashTable = new JavaHashMap[Row, CompactBuffer[Row]]()
    while (iter.hasNext) {
      val currentRow = iter.next()
      numIterRows += 1
      val rowKey = keyGenerator(currentRow)

      var existingMatchList = hashTable.get(rowKey)
      if (existingMatchList == null) {
        existingMatchList = new CompactBuffer[Row]()
        hashTable.put(rowKey, existingMatchList)
      }

      existingMatchList += currentRow.copy()
    }

    hashTable
  }

  protected override def doExecute(): RDD[Row] = {
    val numLeftRows = longMetric("numLeftRows")
    val numRightRows = longMetric("numRightRows")
    val numOutputRows = longMetric("numOutputRows")
    val joinedRow = new JoinedRow()
    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      // TODO this probably can be replaced by external sort (sort merged join?)

      joinType match {
        case LeftOuter =>
          val rightHashTable = buildHashTable(
            rightIter, numRightRows, newProjection(rightKeys, right.output))
          val keyGenerator = newProjection(leftKeys, left.output)
          leftIter.flatMap( currentRow => {
            numLeftRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(
              rowKey, joinedRow, rightHashTable.getOrElse(rowKey, EMPTY_LIST), numOutputRows)
          })

        case RightOuter =>
          val leftHashTable = buildHashTable(
            leftIter, numLeftRows, newProjection(leftKeys, left.output))
          val keyGenerator = newProjection(rightKeys, right.output)
          rightIter.flatMap ( currentRow => {
            numRightRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(
              rowKey, leftHashTable.getOrElse(rowKey, EMPTY_LIST), joinedRow, numOutputRows)
          })

        case FullOuter =>
          val leftHashTable =
            buildHashTable(leftIter, numLeftRows, newProjection(leftKeys, left.output))
          val rightHashTable =
            buildHashTable(rightIter, numRightRows, newProjection(rightKeys, right.output))
          (leftHashTable.keySet ++ rightHashTable.keySet).iterator.flatMap { key =>
            fullOuterIterator(
              key,
              leftHashTable.getOrElse(key, EMPTY_LIST),
              rightHashTable.getOrElse(key, EMPTY_LIST),
              joinedRow,
              numOutputRows)
          }

        case x => throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
      }
    }
  }
}
