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
import java.time.{Duration => JavaDuration}

import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{RuntimeEventState, RuntimeInfo}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}

import scala.io.Codec

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
private[smartdatalake] case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, actionsState: Map[String, RuntimeInfo], isFinal: Boolean) {
  def toJson: String = ActionDAGRunState.toJson(this)
  def isFailed: Boolean = actionsState.exists(_._2.state==RuntimeEventState.FAILED)
  def isSucceeded: Boolean = isFinal && !isFailed
}
private[smartdatalake]object ActionDAGRunState {
  // json4s is used because kxbmap configs supports converting case classes to config only from verion 5.0 which isn't yet stable
  // json4s is included with spark-core
  // Note that key-type of Maps must be String in json4s (e.g. actionsState...)
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  import org.json4s.jackson.Serialization
  implicit val formats =  DefaultFormats.withHints(ShortTypeHints(List(classOf[SparkSubFeed],classOf[FileSubFeed]))).withTypeHintFieldName("type") +
    RuntimeEventStateSerializer + DurationSerializer + LocalDateTimeSerializer

  def toJson(actionDAGRunState: ActionDAGRunState): String = pretty(parse(Serialization.write(actionDAGRunState)))

  def fromJson(stateJson: String): ActionDAGRunState = parse(stateJson).extract[ActionDAGRunState]

  // custom serialization for RuntimeEventState which is shorter than the default
  case object RuntimeEventStateSerializer extends CustomSerializer[RuntimeEventState](format => (
    { case JString(state) => RuntimeEventState.withName(state) },
    { case state: RuntimeEventState => JString(state.toString) }
  ))
  // custom serialization for Duration
  case object DurationSerializer extends CustomSerializer[JavaDuration](format => (
    { case JString(dur) => JavaDuration.parse(dur) },
    { case dur: JavaDuration => JString(dur.toString) }
  ))
  // custom serialization for LocalDateTime
  case object LocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => (
    { case JString(tstmp) => LocalDateTime.parse(tstmp) },
    { case tstmp: LocalDateTime => JString(tstmp.toString) }
  ))
}

private[smartdatalake] trait ActionDAGRunStateStore[A <: StateId] extends SmartDataLakeLogger {

  /**
   * Save State
   */
  def saveState(state: ActionDAGRunState): Unit

  /**
   * Get latest state
   * @param runId optional runId to search for latest state
   */
  def getLatestState(runId: Option[Int] = None): A

  /**
   * Get latest runId
   */
  def getLatestRunId: Option[Int]

  /**
   * recover previous run state
   */
  def recoverRunState(stateId: A): ActionDAGRunState
}

private[smartdatalake] trait StateId
