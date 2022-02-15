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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.{ReflectionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.{ExecutionId, RuntimeEventState, RuntimeInfo, SDLExecutionId}
import org.apache.spark.util.Json4sCompat
import org.json4s._
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, writePretty}

import java.time.{Duration, LocalDateTime}

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime
                                                    , actionsState: Map[ActionId, RuntimeInfo], isFinal: Boolean) {
  def toJson: String = ActionDAGRunState.toJson(this)
  def isFailed: Boolean = actionsState.exists(_._2.state==RuntimeEventState.FAILED)
  def isSucceeded: Boolean = isFinal && !isFailed
  def isSkipped: Boolean = isFinal &&
    actionsState.filter(_._2.executionId.isInstanceOf[SDLExecutionId]).forall(_._2.state==RuntimeEventState.SKIPPED)
  def getDataObjectsState: Seq[DataObjectState] = {
    actionsState.flatMap{ case (actionId, info) => info.dataObjectsState }.toSeq
  }
}
case class DataObjectState(dataObjectId: DataObjectId, state: String) {
  def getEntry: (DataObjectId, DataObjectState) = (dataObjectId, this)
}

private[smartdatalake] object ActionDAGRunState {

  private val durationSerializer = Json4sCompat.getCustomSerializer[Duration](formats => (
    {
      case json: JString => Duration.parse(json.s)
      case json: JInt => Duration.ofSeconds(json.num.toLong)
    },
    {case obj: Duration => JString(obj.toString)}
  ))
  private val localDateTimeSerializer = Json4sCompat.getCustomSerializer[LocalDateTime](formats => (
    {case json: JString => LocalDateTime.parse(json.s)},
    {case obj: LocalDateTime => JString(obj.toString)}
  ))
  private val actionIdSerializer = Json4sCompat.getCustomSerializer[ActionId](formats => (
    {case json: JString => ActionId(json.s)},
    {case obj: ActionId => JString(obj.id)}
  ))
  private val dataObjectIdSerializer = Json4sCompat.getCustomSerializer[DataObjectId](formats => (
    {case json: JString => DataObjectId(json.s)},
    {case obj: DataObjectId => JString(obj.id)}
  ))

  implicit private lazy val workflowReflections = ReflectionUtil.getReflections("io.smartdatalake.workflow")

  private lazy val typeHints = ShortTypeHints(ReflectionUtil.getTraitImplClasses[SubFeed].toList ++ ReflectionUtil.getSealedTraitImplClasses[ExecutionId], "type")
  implicit private val formats: Formats = Json4sCompat.getStrictSerializationFormat(typeHints) + new EnumNameSerializer(RuntimeEventState) +
    actionIdSerializer + dataObjectIdSerializer + durationSerializer + localDateTimeSerializer

  // write state to Json
  def toJson(actionDAGRunState: ActionDAGRunState): String = {
    writePretty(actionDAGRunState)
  }

  // read state from json
  def fromJson(stateJson: String): ActionDAGRunState = {
    try{
      read[ActionDAGRunState](stateJson)
    } catch {
      case ex: Exception => throw new IllegalStateException(s"Unable to parse state from json: ${ex.getMessage}", ex)
    }
  }
}

private[smartdatalake] trait ActionDAGRunStateStore[A <: StateId] extends SmartDataLakeLogger {

  /**
   * Save State
   */
  def saveState(state: ActionDAGRunState): Unit

  /**
   * Get latest state
   * @param runId optional runId to search for latest StateId
   * @return latest StateId for given runId or latest runId, none if it doesn't exist.
   */
  def getLatestStateId(runId: Option[Int] = None): Option[A]

  /**
   * Get latest runId
   */
  def getLatestRunId: Option[Int]

  /**
   * recover previous run state
   */
  def recoverRunState(stateId: A): ActionDAGRunState
}

private[smartdatalake] trait StateId {
  def runId: Int
  def attemptId: Int
}