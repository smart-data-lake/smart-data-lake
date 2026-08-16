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

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.ConfigParser
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.{ReflectionUtil, SmartDataLakeLogger}
import io.smartdatalake.util.json.SdlJsonUtils
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{ExecutionId, RuntimeEventState, RuntimeInfo, SDLExecutionId}
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.{write, writePretty}
import org.reflections.Reflections

import java.time.LocalDateTime

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime,
                             actionsState: Map[ActionId, RuntimeInfo], isFinal: Boolean, runStateFormatVersion: Option[Int],
                             sdlbVersionInfo: Option[Map[String,Any]], appVersionInfo: Option[Map[String,Any]]) {

  def toJson: String = ActionDAGRunState.toJson(this)

  /**
   * Actions which did not reach a successful end state and which a recovery run would execute again.
   *
   * This mirrors [[RuntimeInfo.hasCompleted]], the condition used to decide which Actions are skipped on recovery,
   * so that a run is considered failed exactly if there is something left for a recovery to do. Note that STREAMING
   * is excluded: asynchronous streaming Actions report this state in the final state file of a run which stopped
   * gracefully, and such a run must not be recovered.
   */
  def unfinishedActionsState: Map[ActionId, RuntimeInfo] =
    actionsState.filterNot { case (_, info) => info.hasCompleted || info.state == RuntimeEventState.STREAMING }

  def isFailed: Boolean = unfinishedActionsState.nonEmpty

  def isSucceeded: Boolean = isFinal && !isFailed

  def isSkipped: Boolean = isFinal &&
    actionsState.filter(_._2.executionId.isInstanceOf[SDLExecutionId]).forall(_._2.state == RuntimeEventState.SKIPPED)

  def getDataObjectsState: Seq[DataObjectState] = {
    val dataObjectsState = actionsState.toSeq.flatMap { case (_, info) => info.dataObjectsState }
    val duplicateDataObjectState = dataObjectsState.groupBy(_.dataObjectId).filter(_._2.size > 1)
    assert(duplicateDataObjectState.isEmpty, s"${duplicateDataObjectState.mkString(", ")} is read from multiple Actions with DataObjectStateIncrementalMode. This is not supported.")
    // return
    dataObjectsState
  }

  def finalState: Option[RuntimeEventState] =
    if (!isFinal) {
      None
    } else {
      if (isFailed)
        Some(RuntimeEventState.FAILED)
      else if (isSkipped)
        Some(RuntimeEventState.SKIPPED)
      else if (isSucceeded)
        Some(RuntimeEventState.SUCCEEDED)
      else throw new IllegalStateException("Illegal State")
    }
}
case class DataObjectState(dataObjectId: DataObjectId, state: String) {
  def getEntry: (DataObjectId, DataObjectState) = (dataObjectId, this)
  def toStringTuple: (String,String) = (dataObjectId.id, state)
}

private[smartdatalake] object ActionDAGRunState extends SmartDataLakeLogger {

  // Note: if increasing this version, please check if a StateMigrator is needed to read files of older versions. See also stateMigrators below.
  val runStateFormatVersion: Int = 6

  implicit private lazy val workflowReflections: Reflections = ReflectionUtil.getReflections(ConfigParser.WORKFLOW_PACKAGE)

  private lazy val typeHints = ShortTypeHints(ReflectionUtil.getTraitImplClasses[SubFeed].toList ++ ReflectionUtil.getSealedTraitImplClasses[ExecutionId], "type")
  implicit val formats: Formats = SdlJsonUtils.getFormats(typeHints).strict

  // write state to JSON
  def toJson(actionDAGRunState: ActionDAGRunState): String = {
    writePretty(actionDAGRunState)
  }

  def toJson(info: RuntimeInfo): String = {
    writePretty(info)
  }

  def toJson(entry: IndexEntry): String = {
    // index entry should be written compact in one line (not pretty)
    write(entry)
  }

  // read state from JSON
  def fromJson(stateJson: String): ActionDAGRunState = {
    try{
      val jObj = JsonMethods.parse(stateJson).asInstanceOf[JObject]
      val migratedJObj = checkStateFormatVersionAndMigrate(jObj).getOrElse(jObj)
      // extract into class structures
      migratedJObj.extract[ActionDAGRunState]
    } catch {
      case ex: Exception => throw new IllegalStateException(s"Unable to parse state from json: ${ex.getMessage}", ex)
    }
  }

    def checkStateFormatVersionAndMigrate(json: JObject): Option[JObject] = {
      // convert old format versions
      val formatVersion = json \ "runStateFormatVersion" match {
        case JInt(i) => i.toInt
        case _ => 0 // runStateFormatVersion was missing in first format version
      }
      val appName = json \ "appConfig" \ "applicationName" match {
        case JString(s) => s
        case _ => json \ "appConfig" \ "feedSel" match {
          case JString(s) => s
          case _ => throw new IllegalStateException("Unable to extract applicationName from state json," +
            " neither 'applicationName' nor 'feedSel' field found")
        }
      }
      val runId = json \ "runId" match {
        case JInt(i) => i.toInt
        case _ => throw new IllegalStateException("Expected runId to be an integer in state json")
      }
      val attemptId = json \ "attemptId" match {
        case JInt(i) => i.toInt
        case _ => throw new IllegalStateException("Expected attemptId to be an integer in state json")
      }
     assert(formatVersion <= runStateFormatVersion,
       s"Cannot read state file with formatVersion=$formatVersion newer than the version of this build ($runStateFormatVersion)." +
         s" Check state file app=$appName runId=$runId attemptId=$attemptId and that your SDLB version is up-to-date!")
    // a migrator has to be applied if the state file is not newer than the version it migrates from,
    // e.g. a state file with formatVersion=5 still needs the migrator 5 -> 6.
    val migrators = stateMigrators.dropWhile(m => m.versionFrom < formatVersion)
    if (migrators.nonEmpty) {
      logger.info(s"Applying state migrators ${migrators.mkString(", ")} to state json for app=$appName runId=$runId attemptId=$attemptId")
      Some(migrators.foldLeft(json)((v, m) => m.migrate(v)))
    } else None
  }

  // list of state migrators, sorted in ascending order
  private val stateMigrators: Seq[StateMigratorDef] = Seq(
    new StateMigratorDef3To4(),
    new StateMigratorDef4To5(),
    new StateMigratorDef5To6()
  ).sortBy(_.versionFrom) // force ordering
  assert(stateMigrators.groupBy(_.versionFrom).forall(_._2.size == 1)) // check that versionFrom is unique
  assert(stateMigrators.forall(m => m.versionFrom + 1 == m.versionTo)) // check that a state migrator always converts to the next version, without skipping a version.
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

  /**
   * True if this state was accepted, meaning that the run must not be recovered even if it contains failed Actions.
   * See [[HadoopFileActionDAGRunStateStore]], where a state file is accepted by moving it into the 'succeeded'
   * directory.
   */
  def isAccepted: Boolean = false
}