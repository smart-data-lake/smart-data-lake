/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package io.smartdatalake.metrics

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.SDLExecutionId
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted}

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Collects spark metrics for spark stages filtered by ActionId and an optional DataObjectId
 */
private[smartdatalake] class SparkStageMetricsListener(actionId: ActionId, dataObjectId: Option[DataObjectId] = None)(implicit context: ActionPipelineContext) extends SparkListener with SmartDataLakeLogger {

  /**
   * Keeps track of running jobs, e.g. jobs that started but did not yet end.
   * Map key is jobId.
   */
  val runningJobs: mutable.Map[Int, JobInfo] = mutable.Map.empty

  /**
   * Stores jobID and jobDescription indexed by stage ids.
   */
  val stageIdToJobInfo: mutable.Map[Int, JobInfo] = mutable.Map.empty

  // parses job group
  private val jobGroupRegex = (s"${Regex.quote(context.appConfig.appName)} $actionId runId=([0-9]+) attemptId=([0-9]+)").r.unanchored
  // for spark streaming jobs we cant set the jobGroup, but only the description. They also have no executionId.
  private val jobDescriptionRegex = (s"${Regex.quote(context.appConfig.appName)} $actionId").r.unanchored
  // store collected metrics
  private val metrics: mutable.Buffer[SparkStageMetrics] = mutable.Buffer.empty

  /**
   * On job start, register the job ids and stage ids.
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobGroup = jobStart.properties.getProperty("spark.jobGroup.id")
    val jobDescription = jobStart.properties.getProperty("spark.job.description")
    val jobInfo = (jobGroup, jobDescription) match {
      case (jobGroupRegex(runId, attemptId), _) => Some(JobInfo(jobStart.jobId, jobGroup, jobDescription, Some(SDLExecutionId(runId.toInt, attemptId.toInt))))
      case (_, jobDescriptionRegex()) => Some(JobInfo(jobStart.jobId, jobGroup, jobDescription, None)) // spark streaming job info is read from description, has no executionId information.
      case _ => None
    }
    jobInfo.foreach { i =>
      if (dataObjectId.isEmpty || i.dataObjectId.contains(dataObjectId.get)) {
        runningJobs(i.id) = i
        jobStart.stageIds.foreach(stageId => stageIdToJobInfo(stageId) = i)
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (runningJobs.isDefinedAt(jobEnd.jobId)) synchronized {
      runningJobs.remove(jobEnd.jobId)
      notifyAll() // wakeup waitFor method
    }
  }

  /**
   * On stage complete notify spark metrics
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    // extract basic job information
    val stageId = stageCompleted.stageInfo.stageId
    stageIdToJobInfo.get(stageId).foreach { jobInfo =>
      // extract useful information/metrics
      val taskMetrics = stageCompleted.stageInfo.taskMetrics
      val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
      val sparkStageMetrics = SparkStageMetrics(jobInfo, stageId, stageCompleted.stageInfo.name, stageCompleted.stageInfo.numTasks,
        stageCompleted.stageInfo.submissionTime.getOrElse(-1L), stageCompleted.stageInfo.completionTime.getOrElse(-1L),
        taskMetrics.executorRunTime, taskMetrics.executorCpuTime, taskMetrics.executorDeserializeTime, taskMetrics.executorDeserializeCpuTime,
        taskMetrics.resultSerializationTime, taskMetrics.resultSize,
        taskMetrics.jvmGCTime,
        taskMetrics.memoryBytesSpilled, taskMetrics.diskBytesSpilled,
        taskMetrics.peakExecutionMemory,
        taskMetrics.inputMetrics.bytesRead, taskMetrics.inputMetrics.recordsRead,
        taskMetrics.outputMetrics.bytesWritten, taskMetrics.outputMetrics.recordsWritten,
        shuffleReadMetrics.fetchWaitTime,
        shuffleReadMetrics.remoteBlocksFetched, shuffleReadMetrics.localBlocksFetched, shuffleReadMetrics.totalBlocksFetched,
        shuffleReadMetrics.remoteBytesRead, shuffleReadMetrics.localBytesRead, shuffleReadMetrics.totalBytesRead,
        shuffleReadMetrics.recordsRead,
        taskMetrics.shuffleWriteMetrics.writeTime, taskMetrics.shuffleWriteMetrics.bytesWritten, taskMetrics.shuffleWriteMetrics.recordsWritten,
        stageCompleted.stageInfo.accumulables.values.toSeq
      )
      metrics.append(sparkStageMetrics)
    }
  }

  def register: SparkStageMetricsListener = {
    context.sparkSession.sparkContext.addSparkListener(this)
    this
  }

  /**
   * Waits until all jobs are ended, and returns relevant metrics.
   * Note that this should be called when Spark Jobs are finished, but through the asynchronous nature of Spark listeners this might be a little bit late.
   */
  def waitForSparkMetrics(timeoutSec: Int = 10): Seq[SparkStageMetrics] = try {
    synchronized {
      // we need to loop as wait might return without us calling notify
      // https://en.wikipedia.org/w/index.php?title=Spurious_wakeup&oldid=992601610
      val ts = System.currentTimeMillis()
      while (runningJobs.nonEmpty) {
        logger.debug(s"waiting for jobIds=${runningJobs.keys.mkString(",")}")
        wait(timeoutSec * 1000L)
        if (ts + timeoutSec * 1000L <= System.currentTimeMillis) throw SparkJobNotEndedException(s"SparkStageMetricsListener didn't get onJobEnd notification for jobIds=${runningJobs.keys.mkString(",")} within timeout of $timeoutSec seconds.")
      }
    }
    metrics.toSeq
  } finally {
    context.sparkSession.sparkContext.removeSparkListener(this)
  }

  /**
   * Wait for Spark metrics and return main metrics from latest job.
   */
  def waitForLastMetrics(timeoutSec: Int = 10): MetricsMap = {
    waitForSparkMetrics(timeoutSec).sortBy(_.jobInfo.id).lastOption.map(_.getMainInfos).getOrElse(Map())
  }
}

object SparkStageMetricsListener {

  /**
   * Executes a Spark statement and return metrics collected through listener
   */
  def execWithMetrics(dataObjectId: DataObjectId, sparkCode: => Unit)(implicit context: ActionPipelineContext): MetricsMap = {
    // setup listener
    val stageMetricsListener = context.currentAction.map(action => new SparkStageMetricsListener(action.id, Some(dataObjectId)).register)
    // exec spark code
    sparkCode
    // return metrics
    stageMetricsListener.map(_.waitForLastMetrics()).getOrElse(Map())
  }
}

case class SparkJobNotEndedException(msg: String) extends Exception(msg)
