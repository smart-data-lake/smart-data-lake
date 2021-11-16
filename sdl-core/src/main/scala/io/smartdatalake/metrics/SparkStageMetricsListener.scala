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

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import io.smartdatalake.workflow.{ActionMetrics, ActionPipelineContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Collects spark metrics for spark stages.
 */
private[smartdatalake] class SparkStageMetricsListener(action: Action)(implicit context: ActionPipelineContext) extends SparkListener with SmartDataLakeLogger {

  /**
   * Stores jobID and jobDescription indexed by stage ids.
   */
  val jobInfoLookupTable: mutable.Map[Int, JobInfo] = mutable.Map.empty

  // parses job group
  private val jobGroupRegex = (s"${Regex.quote(context.appConfig.appName)} ${action.id} runId=([0-9]+) attemptId=([0-9]+)").r.unanchored
  // for spark streaming jobs we cant set the jobGroup, but only the description. They also have no executionId.
  private val jobDescriptionRegex = (s"${Regex.quote(context.appConfig.appName)} ${action.id}").r.unanchored

  /**
   * On job start, register the job ids and stage ids.
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobGroup = jobStart.properties.getProperty("spark.jobGroup.id")
    val jobDescription = jobStart.properties.getProperty("spark.job.description")
    val jobInfo = (jobGroup, jobDescription) match {
      case (jobGroupRegex(runId,attemptId), _) => Some(JobInfo(jobStart.jobId, jobGroup, jobDescription, Some(SDLExecutionId(runId.toInt, attemptId.toInt))))
      case (_, jobDescriptionRegex()) => Some(JobInfo(jobStart.jobId, jobGroup, jobDescription, None)) // spark streaming job info is read from description, has no executionId information.
      case _ => None
    }
    jobInfo.foreach(i => jobStart.stageIds.foreach(stageId => jobInfoLookupTable(stageId) = i))
  }

  /**
   * On stage complete notify spark metrics
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    // extract basic job information
    val stageId = stageCompleted.stageInfo.stageId
    jobInfoLookupTable.get(stageId).foreach { jobInfo =>
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
      val dataObjectIdRegex = (s"DataObject~(${SdlConfigObject.idRegexStr})").r.unanchored
      val dataObjectId = jobInfo.description match {
        case dataObjectIdRegex(id) => Some(DataObjectId(id))
        case _ => None // there are some stages which are created by Spark DataFrame operations which dont manipulate Actions target DataObject's, e.g. pivot operator
      }
      // note that executionId might be empty for asynchronous actions
      action.addRuntimeMetrics(jobInfo.executionId, dataObjectId, sparkStageMetrics)
    }
  }
}
