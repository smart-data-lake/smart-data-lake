/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow

import java.time.LocalDateTime
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable

/**
 * ActionPipelineContext contains start and runtime information about a SmartDataLake run.
 *
 * @param feed feed selector of the run
 * @param application application name of the run
 * @param executionId SDLExecutionId of this runs. Contains runId and attemptId. Both stay 1 if state is not enabled.
 * @param instanceRegistry registry of all SmartDataLake objects parsed from the config
 * @param referenceTimestamp timestamp used as reference in certain actions (e.g. HistorizeAction)
 * @param appConfig the command line parameters parsed into a [[SmartDataLakeBuilderConfig]] object
 * @param runStartTime start time of the run
 * @param attemptStartTime start time of attempt
 * @param simulation true if this is a simulation run
 * @param phase current execution phase
 * @param dataFrameReuseStatistics Counter how many times a DataFrame of a SparkSubFeed is reused by an Action later in the pipeline.
 *                                 The counter is increased during ExecutionPhase.Init when preparing the SubFeeds for an Action and it is
 *                                 decreased in ExecutionPhase.Exec to unpersist the DataFrame after there is no need for it anymore.
 * @param actionsSelected actions selected for execution by command line parameter --feed-sel
 * @param actionsSkipped actions selected but skipped in current attempt because they already succeeded in a previous attempt.
 */
@DeveloperApi
case class ActionPipelineContext (
                                  feed: String, application: String, executionId: SDLExecutionId,
                                  instanceRegistry: InstanceRegistry,
                                  referenceTimestamp: Option[LocalDateTime] = None,
                                  appConfig: SmartDataLakeBuilderConfig, // application config is needed to persist action dag state for recovery
                                  runStartTime: LocalDateTime = LocalDateTime.now(),
                                  attemptStartTime: LocalDateTime = LocalDateTime.now(),
                                  simulation: Boolean = false,
                                  var phase: ExecutionPhase = ExecutionPhase.Prepare,
                                  dataFrameReuseStatistics: mutable.Map[(DataObjectId, Seq[PartitionValues]), Seq[ActionId]] = mutable.Map(),
                                  actionsSelected: Seq[ActionId] = Seq(),
                                  actionsSkipped: Seq[ActionId] = Seq()
) extends SmartDataLakeLogger {
  private[smartdatalake] def getReferenceTimestampOrNow: LocalDateTime = referenceTimestamp.getOrElse(LocalDateTime.now)
  private[smartdatalake] def rememberDataFrameReuse(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], actionId: ActionId): Int = dataFrameReuseStatistics.synchronized {
    val key = (dataObjectId, partitionValues)
    val newValue = dataFrameReuseStatistics.getOrElse(key, Seq()) :+ actionId
    dataFrameReuseStatistics.update(key, newValue)
    newValue.size
  }
  private[smartdatalake] def forgetDataFrameReuse(dataObjectId: DataObjectId, partitionValues: Seq[PartitionValues], actionId: ActionId): Option[Int] = dataFrameReuseStatistics.synchronized {
    val key = (dataObjectId, partitionValues)
    val existingValue = dataFrameReuseStatistics.get(key)
    existingValue.map { v =>
      val newValue = v.diff(Seq(actionId))
      if (v.size == newValue.size) logger.warn(s"Could not find $actionId in dataFrame reuse list!")
      dataFrameReuseStatistics.update(key, newValue)
      newValue.size
    }
  }
  // manage executionId
  private[smartdatalake] def incrementRunId = this.copy(executionId = this.executionId.incrementRunId, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now)
  private[smartdatalake] def incrementAttemptId = this.copy(executionId = this.executionId.incrementAttemptId, attemptStartTime = LocalDateTime.now)
}
