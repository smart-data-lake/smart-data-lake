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

import io.smartdatalake.app.{BuildVersionInfo, SmartDataLakeBuilderConfig}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ReflectionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{ExecutionId, RuntimeEventState, RuntimeInfo, SDLExecutionId}
import org.apache.spark.util.Json4sCompat
import org.json4s.Extraction.decompose
import org.json4s._
import org.json4s.ext.EnumNameSerializer
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.{write, writePretty}
import org.reflections.Reflections

import java.time.{Duration, LocalDateTime}

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime,
                             actionsState: Map[ActionId, RuntimeInfo], isFinal: Boolean, runStateFormatVersion: Option[Int],
                             buildVersionInfo: Option[BuildVersionInfo], appVersion: Option[String]) {

  def toJson: String = ActionDAGRunState.toJson(this)

  def isFailed: Boolean = actionsState.exists(_._2.state == RuntimeEventState.FAILED)

  def isSucceeded: Boolean = isFinal && !isFailed

  def isSkipped: Boolean = isFinal &&
    actionsState.filter(_._2.executionId.isInstanceOf[SDLExecutionId]).forall(_._2.state == RuntimeEventState.SKIPPED)

  def getDataObjectsState: Seq[DataObjectState] = {
    val dataObjectsState = actionsState.toSeq.flatMap { case (actionId, info) => info.dataObjectsState }
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
  val runStateFormatVersion: Int = 4

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
  private val actionIdKeySerializer = Json4sCompat.getCustomKeySerializer[ActionId](formats => (
    {case s: String => ActionId(s)},
    {case obj: ActionId => obj.id}
  ))
  private val dataObjectIdKeySerializer = Json4sCompat.getCustomKeySerializer[DataObjectId](formats => (
    {case s: String => DataObjectId(s)},
    {case obj: DataObjectId => obj.id}
  ))
  private val dataObjectIdSerializer = Json4sCompat.getCustomSerializer[DataObjectId](formats => (
    {case json: JString => DataObjectId(json.s)},
    {case obj: DataObjectId => JString(obj.id)}
  ))
  private val runtimeEventStateKeySerializer = Json4sCompat.getCustomKeySerializer[RuntimeEventState](formats => (
    {case s: String => RuntimeEventState.withName(s)},
    {case obj: RuntimeEventState => obj.toString}
  ))
  private val partitionValuesSerializer = Json4sCompat.getCustomSerializer[PartitionValues](implicit formats => (
    {case json: JObject => PartitionValues(json.values)},
    {case obj: PartitionValues => JObject(obj.elements.map(e => JField(e._1, decompose(e._2))).toList)}
  ))

  implicit private lazy val workflowReflections: Reflections = ReflectionUtil.getReflections("io.smartdatalake.workflow")

  private lazy val typeHints = ShortTypeHints(ReflectionUtil.getTraitImplClasses[SubFeed].toList ++ ReflectionUtil.getSealedTraitImplClasses[ExecutionId], "type")
  implicit val formats: Formats = Json4sCompat.getStrictSerializationFormat(typeHints) + new EnumNameSerializer(RuntimeEventState) +
    actionIdKeySerializer + dataObjectIdKeySerializer + dataObjectIdSerializer + durationSerializer + localDateTimeSerializer + runtimeEventStateKeySerializer + partitionValuesSerializer

  // write state to Json
  def toJson(actionDAGRunState: ActionDAGRunState): String = {
    writePretty(actionDAGRunState)
  }

  def toJson(entry: IndexEntry): String = {
    // index entry should be written compact in one line (not pretty)
    write(entry)
  }

  // read state from json
  def fromJson(stateJson: String): ActionDAGRunState = {
    try{
      val jValue = JsonMethods.parse(stateJson)
      val migratedJValue = checkStateFormatVersionAndMigrate(jValue).getOrElse(jValue)
      // extract into class structures
      migratedJValue.extract[ActionDAGRunState]
    } catch {
      case ex: Exception => throw new IllegalStateException(s"Unable to parse state from json: ${ex.getMessage}", ex)
    }
  }

  def checkStateFormatVersionAndMigrate(jValue: JValue): Option[JValue] = {
    // convert old format versions
    val formatVersion = jValue \ "runStateFormatVersion" match {
      case JInt(i) => i.toInt
      case _ => 0 // runStateFormatVersion was missing in first format version
    }
    val appName = jValue \ "appConfig" \ "applicationName" match {
      case JString(s) => s
      case _ => jValue \ "appConfig" \ "feedSel" match {
        case JString(s) => s
      }
    }
    val runId = jValue \ "runId" match {
      case JInt(i) => i.toInt
    }
    val attemptId = jValue \ "attemptId" match {
      case JInt(i) => i.toInt
    }
    assert(formatVersion <= runStateFormatVersion, s"Cannot read state file with formatVersion=${formatVersion} newer than the version of this build (${runStateFormatVersion}). Check state file app=$appName runId=$runId attemptId=$attemptId and that your SDLB version is up-to-date!")
    val migrators = stateMigrators.dropWhile(m => m.versionFrom <= formatVersion)
    if (migrators.nonEmpty) {
      logger.info(s"Applying state migrators ${migrators.mkString(", ")} to state json for app=$appName runId=$runId attemptId=$attemptId")
      Some(migrators.foldLeft(jValue)((v, m) => m.migrate(v)))
    } else None
  }

  // list of state migrators, sorted in ascending order
  private val stateMigrators: Seq[StateMigratorDef] = Seq(
    new StateMigratorDef3To4()
  ).sortBy(_.versionFrom) // force ordering
  assert(stateMigrators.groupBy(_.versionFrom).forall(_._2.size == 1)) // check that versionFrom is unigue
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
}