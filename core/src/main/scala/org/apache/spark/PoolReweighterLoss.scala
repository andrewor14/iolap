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

package org.apache.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler._


private case class PRJob(jobId: Long, poolName: String, stageIds: Seq[Int])

class PRBatchWindow {
  var durationMs: Long = _
  var loss: Double = _
  var dLoss: Double = _
}

/**
 * Singleton entry point to SLAQ utility scheduling.
 */
object PoolReweighterLoss extends Logging {

  private val batchWindows = new mutable.HashMap[String, ArrayBuffer[PRBatchWindow]]
  private val pool2numCores = new mutable.HashMap[String, Int]
  private lazy val sc = SparkContext.getOrCreate()
  val listener = new PRJobListener
  val tokens = new mutable.HashMap[String, Long]

  private var batchIntervalMs = 0
  private var isFair = false
  @volatile var isRunning = false

  /**
   * Whether there are active pools registered with this object.
   */
  def hasRegisteredPools: Boolean = pool2numCores.nonEmpty

  /**
   * Called by application code to report loss values for a given pool periodically.
   * This must be called after the pool has registered with this object and has run
   * at least one task.
   */
  def updateLoss(loss: Double): Unit = {
    val poolName = sc.getLocalProperty("spark.scheduler.pool")
    logInfo(s"LOGAN: $poolName curr loss: $loss")
    val bws = batchWindows(poolName)
    bws.append(listener.currentWindows(poolName))
    // Update loss and delta loss, if any, in most recent batch window
    bws.last.loss = loss
    if (bws.size >= 2) {
      bws.last.dLoss = bws(bws.size - 2).loss - bws.last.loss
    }
    listener.currentWindows.put(poolName, new PRBatchWindow)
  }

  /**
   * Signal that no more losses will be reported.
   */
  def done(poolName: String): Unit = {
    pool2numCores.remove(poolName)
    batchWindows.remove(poolName)
    tokens.remove(poolName)
    listener.currentWindows.remove(poolName)
    listener.avgTaskTime.remove(poolName)
  }

  /**
   * Stop the thread that assigns tokens periodically to each pool.
   */
  def stop(): Unit = {
    isRunning = false
  }

  /**
   * Start the thread that assigns tokens periodically to each pool.
   * This must be called before [[register]].
   */
  def start(t: Int = 10, fair: Boolean = false): Unit = {
    // Fail fast against wrong scheduling mode, otherwise token accounting would be incorrect
    if (sc.taskScheduler.rootPool.schedulingMode != SchedulingMode.FAIR) {
      throw new IllegalStateException("Please set 'spark.scheduler.mode' to 'FAIR'!")
    }
    sc.addSparkListener(listener)
    isFair = fair
    batchIntervalMs = t * 1000
    isRunning = true
    val thread = new Thread {
      override def run(): Unit = {
        while (isRunning) {
          Thread.sleep(batchIntervalMs)
          assignTokens()
          printTokens()
        }
      }
    }
    thread.start()
  }

  /**
   * Register a pool with the scheduler and kick off a round of token assignment.
   */
  def register(poolName: String): Unit = {
    pool2numCores.put(poolName, -1)
    batchWindows.put(poolName, new ArrayBuffer[PRBatchWindow])
    tokens.put(poolName, 0)
    assignTokens()
  }

  /**
   * Assign tokens to all registered pools.
   *
   * This is the core of the scheduling logic. The number of tokens assigned to a
   * pool is proportional to how often tasks belonging to the pool are launched.
   * At the end of each task, the average task time across all tasks running in
   * the pool are subtracted from the number of tokens assigned.
   *
   * There are two scheduling modes. In fair scheduling, the same number of tokens
   * are assigned to all the pools. In utility scheduling, the number of tokens
   * assigned to a pool is proportional to the amount of utility increase the pool
   * is expected to have by the next scheduling batch.
   */
  private def assignTokens(): Unit = {
    if (pool2numCores.isEmpty) {
      return
    }
    if (isFair) {
      // This is fair scheduling. We assign the same number of tokens to each pool.
      // The specific number of tokens used here is not important.
      for ((poolName: String, numCores: Int) <- pool2numCores) {
        tokens(poolName) = batchIntervalMs
      }
    } else {
      // This is utility scheduling. We use a heap to rank the pools based on how much
      // utility improvement each pool will experience from one additional core.
      def diff(t: (String, Double)) = t._2
      val heap = new mutable.PriorityQueue[(String, Double)]()(Ordering.by(diff))
      val totalCores = sc.defaultParallelism
      val fairshare = totalCores / pool2numCores.size
      val minCore = 3

      // Initialize all pools with min cores.
      // If a pool does not have enough losses, just initialize it to the fair share.
      // Note: This does not currently handle the case when number of pools * min cores
      // exceeds the total number of cores in the cluster, in which case remaining cores
      // may fall below zero and behavior is undefined.
      var remainingCores = totalCores
      for ((poolName: String, _) <- pool2numCores) {
        if (batchWindows(poolName).size <= 1) {
          pool2numCores(poolName) = fairshare
          remainingCores -= fairshare
        } else {
          // Initialize heap with amount of utility increase from one additional core
          val util1 = predictNormalizedDeltaLoss(poolName, minCore)
          val util2 = predictNormalizedDeltaLoss(poolName, minCore + 1)
          heap.enqueue((poolName, util2 - util1))
          pool2numCores(poolName) = minCore
          remainingCores -= minCore
        }
      }

      // Assign num cores to pools
      while (remainingCores > 0 && heap.nonEmpty) {
        val (poolName, _) = heap.dequeue()
        pool2numCores(poolName) += 1
        val alloc = pool2numCores(poolName)
        val utilCurr = predictNormalizedDeltaLoss(poolName, alloc)
        val utilNext = predictNormalizedDeltaLoss(poolName, alloc + 1)
        heap.enqueue((poolName, utilNext - utilCurr))
        remainingCores -= 1
      }

      // Convert num cores to tokens
      for ((poolName: String, numCores: Int) <- pool2numCores) {
        tokens(poolName) = batchIntervalMs * numCores
      }
    }

  }

  /**
   * Print number of tokens assigned to each pool.
   */
  private def printTokens(): Unit = {
    if (tokens.nonEmpty) {
      val hashes = "##########################################################"
      val timeString = s"Time = ${System.currentTimeMillis()}"
      val tokenString = tokens.toArray
        .sortBy { case (poolName, _) => poolName }
        .map { case (poolName, numTokens) => s"$poolName = $numTokens" }
        .mkString("\n")
      println(s"\n\n\n$hashes\n$timeString\n$tokenString\n$hashes\n\n\n")
    }
  }

  /**
   * Predict loss reported by a pool after a specified number of iterations.
   * Currently this simply fits a geometric curve to the loss function.
   */
  private def predictLoss(poolName: String, numIterations: Int): Double = {
    val bws = batchWindows(poolName)
    if (bws.size > 1) {
      val lastLoss = bws.last.loss
      val nextLastLoss = bws(bws.size - 2).loss
      val a = lastLoss / nextLastLoss
      lastLoss * Math.pow(a, numIterations)
    } else {
      0.0
    }
  }

  /**
   * Predict delta loss reported by a pool by the next scheduling batch if it were
   * assigned the specified number of cores. The delta loss here is normalized by
   * the max delta loss observed in the pool so far.
   */
  private def predictNormalizedDeltaLoss(poolName: String, numCores: Int): Double = {
    val numLosses = batchWindows(poolName).size
    if (numLosses > 1) {
      // Ignore first iteration because it may be noisy
      val avgIterDuration = batchWindows(poolName).drop(1).map(_.durationMs).sum / numLosses
      val numItersByNextBatch = (numCores * batchIntervalMs / avgIterDuration).toInt
      val predictedLoss = predictLoss(poolName, numItersByNextBatch)
      val maxDeltaLoss = batchWindows(poolName).map(_.dLoss).max
      (batchWindows(poolName).last.loss - predictedLoss) / maxDeltaLoss
    } else {
      0.0
    }
  }

}


/**
 * Listener that keeps track of task duration statistics for each pool.
 */
class PRJobListener extends SparkListener with Logging {
  private val currentJobs = new mutable.HashMap[Long, PRJob]
  val currentWindows = new mutable.HashMap[String, PRBatchWindow]
  val avgTaskTime = new mutable.HashMap[String, (Long, Int)]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val poolName = jobStart.properties.getProperty("spark.scheduler.pool")
    if (poolName != null && poolName != "default") {
      val jobId = jobStart.jobId
      val job = PRJob(jobId, poolName, jobStart.stageIds.toList)
      currentJobs.put(jobId, job)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    if (currentJobs.contains(jobId)) {
      currentJobs.remove(jobId)
    }
  }

  /**
   * Update task duration statistics in the pool to which this task belongs.
   */
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val stageId = taskEnd.stageId
    val taskDurationMs =
      (taskEnd.taskMetrics.executorCpuTime +
      taskEnd.taskMetrics.resultSerializationTime +
      taskEnd.taskMetrics.executorDeserializeCpuTime) / 1000000
    currentJobs.values.foreach { job =>
      if (job.stageIds.contains(stageId)) {
        val poolName = job.poolName
        // Update duration in the current batch window
        if (!currentWindows.contains(poolName)) {
          currentWindows.put(poolName, new PRBatchWindow)
        }
        currentWindows(poolName).durationMs += taskDurationMs
        // Update average task time
        if (!avgTaskTime.contains(poolName)) {
          avgTaskTime.put(poolName, (0, 0))
        }
        val (existingDurationMs, numTasks) = avgTaskTime(poolName)
        avgTaskTime(poolName) = (existingDurationMs + taskDurationMs, numTasks + 1)
      }
    }
  }

}
