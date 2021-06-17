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

import java.time.{LocalDateTime, Duration => JavaDuration}
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{RuntimeEventState, RuntimeInfo}
import com.typesafe.config.{ConfigException, ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import configs.{ConfigKeyNaming, ConfigObject, ConfigReader, ConfigValue, ConfigWriter, Result, StringConverter}
import configs.Result.{Failure, Success}
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.hdfs.PartitionValues

import java.net.InetAddress
import scala.collection.generic.CanBuildFrom

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
private[smartdatalake] case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime
                                                    , actionsState: Map[ActionId, RuntimeInfo], isFinal: Boolean) {
  def toJson: String = ActionDAGRunState.toJson(this)
  def isFailed: Boolean = actionsState.exists(_._2.state==RuntimeEventState.FAILED)
  def isSucceeded: Boolean = isFinal && !isFailed
}
private[smartdatalake] object ActionDAGRunState {

  import configs.syntax._

  def localDateTime(): ConfigReader[LocalDateTime] =
    ConfigReader.fromTry { (c, p) =>
      val s = c.getString(p)
      try LocalDateTime.parse(s) catch {
        case e: Exception =>
          throw new ConfigException.WrongType(c.origin(), p, "LocalDateTime", s"STRING value '$s'", e)
      }
    }
  implicit lazy val localDateTimeConfigReader: ConfigReader[LocalDateTime] = localDateTime

  def actionId(): ConfigReader[ActionId] =
    ConfigReader.fromTry { (c,p) =>
      ActionId(c.getString(p))
    }
  implicit lazy val actionIdConfigReader: ConfigReader[ActionId] = actionId

  def runtimeEventState(): ConfigReader[RuntimeEventState] =
    ConfigReader.fromTry { (c,p) =>
      RuntimeEventState.withName(c.getString(p))
    }
  implicit lazy val runtimeEventStateConfigReader: ConfigReader[RuntimeEventState] = runtimeEventState

  // TODO: Dummy implementation
  def partitionValues(): ConfigReader[PartitionValues] =
    ConfigReader.fromTry { (c,p) =>
      PartitionValues(Map("1"->5))
    }
  implicit lazy val partitionValuesConfigReader: ConfigReader[PartitionValues] = partitionValues

  // TODO: Dummy implementation
  def runtimeInfo(): ConfigReader[RuntimeInfo] =
    ConfigReader.fromTry { (c,p) =>
      RuntimeInfo(RuntimeEventState.STARTED)
    }
  implicit lazy val runtimeInfoConfigReader: ConfigReader[RuntimeInfo] = runtimeInfo

  val any: ConfigWriter[Any] = ConfigValueFactory.fromAnyRef(_)
  def fromAny[A]: ConfigWriter[A] = any.asInstanceOf[ConfigWriter[A]]
  implicit val stringConfigWriter: ConfigWriter[String] = fromAny[String]
  implicit val localDateTimeConfigWriter: ConfigWriter[LocalDateTime] = stringConfigWriter.contramap(_.toString)
  implicit val actionIdConfigWriter: ConfigWriter[ActionId] = stringConfigWriter.contramap(_.id)
  // TODO: Implement
  implicit val runtimeEventStateConfigWriter: ConfigWriter[RuntimeEventState] = stringConfigWriter.contramap(_.toString)
  implicit val runtimeInfoConfigWriter: ConfigWriter[RuntimeInfo] = stringConfigWriter.contramap(_.toString)
  implicit val partitionValuesConfigWriter: ConfigWriter[PartitionValues] = stringConfigWriter.contramap(_.toString)

//  implicit val runtimeInfoConfigWriter: ConfigWriter[RuntimeInfo] = stringConfigWriter.contramap(_.state.toString)

  implicit val stringStringConverter: StringConverter[String] =
    new StringConverter[String] {
      def fromString(string: String): Result[String] = Result.successful(string)
      def toString(value: String): String = value
    }
  implicit val actionIdStringConverter: StringConverter[ActionId] = stringStringConverter.xmap(ActionId.apply, _.id)

  def toJson(actionDAGRunState: ActionDAGRunState): String = {

    implicit val naming = ConfigKeyNaming.lowerCamelCase[ActionDAGRunState].or(ConfigKeyNaming.hyphenSeparated[ActionDAGRunState].apply)
    val renderOptions = ConfigRenderOptions.defaults().setJson(true).setFormatted(true)
    val writer = ConfigWriter.derive[ActionDAGRunState](naming)

    writer.write(actionDAGRunState).render(renderOptions)
  }

  def fromJson(stateJson: String): ActionDAGRunState = {
    ConfigFactory.parseString(stateJson).extract[ActionDAGRunState] match {
      case Success(value) => value
      case Failure(e) => throw new ConfigurationException(s"Unable to parse state from json: ${e.messages.mkString(",")}")
      case _ => throw new Exception("Unmatched case, should never happen.")
    }
  }



  // custom serialization for RuntimeEventState which is shorter than the default
//  case object RuntimeEventStateSerializer extends CustomSerializer[RuntimeEventState](format => (
//    { case JString(state) => RuntimeEventState.withName(state) },
//    { case state: RuntimeEventState => JString(state.toString) }
//  ))
//  // custom serialization for Duration
//  case object DurationSerializer extends CustomSerializer[JavaDuration](format => (
//    { case JString(dur) => JavaDuration.parse(dur) },
//    { case dur: JavaDuration => JString(dur.toString) }
//  ))
//  // custom serialization for LocalDateTime
//  case object LocalDateTimeSerializer extends CustomSerializer[LocalDateTime](format => (
//    { case JString(tstmp) => LocalDateTime.parse(tstmp) },
//    { case tstmp: LocalDateTime => JString(tstmp.toString) }
//  ))
//  // custom serialization for ActionId as key of Maps
//  case object ActionIdKeySerializer extends CustomKeySerializer[ActionId](format => (
//    { case id => ActionId(id) },
//    { case id: ActionId => id.id }
//  ))
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

private[smartdatalake] trait StateId {
  def runId: Int
  def attemptId: Int
}