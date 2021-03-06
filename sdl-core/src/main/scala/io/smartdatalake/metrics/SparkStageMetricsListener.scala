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
import io.smartdatalake.workflow.ActionMetrics
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted}

import scala.collection.mutable

/**
 * Collects spark metrics for spark stages.
 */
private[smartdatalake] class SparkStageMetricsListener(notifyStageMetricsFunc: (ActionId, Option[DataObjectId], ActionMetrics) => Unit) extends SparkListener with SmartDataLakeLogger {

  /**
   * Stores jobID and jobDescription indexed by stage ids.
   */
  val jobInfoLookupTable: mutable.Map[Int, JobInfo] = mutable.Map.empty

  /**
   * On job start, register the job ids and stage ids.
   */
  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageIds.foreach { stageId =>
      jobInfoLookupTable(stageId) = JobInfo(jobStart.jobId, jobStart.properties.getProperty("spark.jobGroup.id"), jobStart.properties.getProperty("spark.job.description"))
    }
  }

  /**
   * On stage complete notify spark metrics
   */
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    // extract useful informations/metrics
    val stageId = stageCompleted.stageInfo.stageId
    val taskMetrics = stageCompleted.stageInfo.taskMetrics
    val shuffleReadMetrics = taskMetrics.shuffleReadMetrics
    val jobInfo = jobInfoLookupTable(stageId)
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
    // extract concerned Action and DataObject
    val actionIdRegex = "Action~([a-zA-Z0-9_-]+)".r.unanchored
    val actionId = sparkStageMetrics.jobInfo.group match {
      case actionIdRegex(id) => Some(ActionId(id))
      case _ => sparkStageMetrics.jobInfo.description match { // for spark streaming jobs we cant set the jobGroup, but only the description
        case actionIdRegex(id) => Some(ActionId(id))
        case _ =>
          logger.warn(s"Couldn't extract ActionId from sparkJobGroupId (${sparkStageMetrics.jobInfo.group})")
          None
      }
    }
    if (actionId.isDefined) {
      val dataObjectIdRegex = "DataObject~([a-zA-Z0-9_-]+)".r.unanchored
      val dataObjectId = sparkStageMetrics.jobInfo.description match {
        case dataObjectIdRegex(id) => Some(DataObjectId(id))
        case _ => sparkStageMetrics.jobInfo.description match { // for spark streaming jobs we cant set the jobGroup, but only the description
          case dataObjectIdRegex(id) => Some(DataObjectId(id))
          case _ => None // there are some stages which are created by Spark DataFrame operations which dont belong to manipulation Actions target DataObject's, e.g. pivot operator
        }
      }
      notifyStageMetricsFunc(actionId.get, dataObjectId, sparkStageMetrics)
    }
  }
}
