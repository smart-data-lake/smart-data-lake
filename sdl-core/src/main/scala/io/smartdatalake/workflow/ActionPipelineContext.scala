/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.app.{GlobalConfig, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{SerializableHadoopConfiguration, SmartDataLakeLogger}
import io.smartdatalake.workflow.ExecutionPhase.ExecutionPhase
import io.smartdatalake.workflow.action.{Action, SDLExecutionId}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import org.apache.hadoop.conf.Configuration

import java.time.LocalDateTime

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
 * @param cacheRegistry Keeps track of DataFrames cached by Actions with cacheOutput=true, so that they can be released
 *                      again once no Action needs them anymore. Consumers are registered during ExecutionPhase.Init,
 *                      the caches are created and released during ExecutionPhase.Exec.
 * @param runtimeRegistry Keeps the runtime state (events & metrics) of all Actions of this execution. As `copy` is
 *                        shallow, all derived contexts share the same registry instance, so runtime information of an
 *                        Action is visible to all Actions executed afterwards.
 * @param actionsSelected actions selected for execution by command line parameter --feed-sel
 * @param actionsSkipped actions selected but skipped in current attempt because they already succeeded in a previous attempt.
 * @param currentAction the Action currently being executed, set by [[withAction]].
 * @param predecessorActions all Actions the currentAction transitively depends on in the DAG, set by [[withAction]].
 *                           These have all finished when the currentAction runs, so their runtime information is
 *                           complete and independent of the scheduling order of parallel branches.
 */
case class ActionPipelineContext (
                                   feed: String, application: String, executionId: SDLExecutionId,
                                   @transient
                                   instanceRegistry: InstanceRegistry,
                                   referenceTimestamp: Option[LocalDateTime] = None,
                                   appConfig: SmartDataLakeBuilderConfig, // application config is needed to persist action dag state for recovery
                                   runStartTime: LocalDateTime = LocalDateTime.now(),
                                   attemptStartTime: LocalDateTime = LocalDateTime.now(),
                                   simulation: Boolean = false,
                                   phase: ExecutionPhase = ExecutionPhase.Prepare,
                                   cacheRegistry: DataFrameCacheRegistry = new DataFrameCacheRegistry(),
                                   runtimeRegistry: ActionsRuntimeRegistry = new ActionsRuntimeRegistry(),
                                   actionsSelected: Seq[ActionId] = Seq(),
                                   actionsSkipped: Seq[ActionId] = Seq(),
                                   globalConfig: GlobalConfig,
                                   currentAction: Option[Action] = None,
                                   @transient
                                   predecessorActions: Seq[Action] = Seq(),
                                 ) extends SmartDataLakeLogger {

  def withAction(action: Action, predecessorActions: Seq[Action] = Seq()): ActionPipelineContext =
    this.copy(currentAction = Some(action), predecessorActions = predecessorActions)

  def engineConnection: Option[Connection with EngineConnection] = {
    currentAction.map(_.getEngineConnection(instanceRegistry))
  }

  def getReferenceTimestampOrNow: LocalDateTime = referenceTimestamp.getOrElse(LocalDateTime.now)

  def isExecPhase: Boolean = phase == ExecutionPhase.Exec

  // manage executionId
  def incrementRunId: ActionPipelineContext = this.copy(executionId = this.executionId.incrementRunId, runStartTime = LocalDateTime.now, attemptStartTime = LocalDateTime.now)

  def incrementAttemptId: ActionPipelineContext = this.copy(executionId = this.executionId.incrementAttemptId, attemptStartTime = LocalDateTime.now)

  /**
   * helper method to access hadoop configuration
   */
  def hadoopConf: Configuration = serializableHadoopConf.get

  val serializableHadoopConf: SerializableHadoopConfiguration = new SerializableHadoopConfiguration(globalConfig.getHadoopConfiguration)

}