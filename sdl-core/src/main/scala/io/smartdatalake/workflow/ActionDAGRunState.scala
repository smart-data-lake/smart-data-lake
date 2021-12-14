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

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions, ConfigValueFactory}
import configs.Result.{Failure, Success}
import configs._
import io.smartdatalake.app.SmartDataLakeBuilderConfig
import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.RuntimeEventState.RuntimeEventState
import io.smartdatalake.workflow.action.{ResultRuntimeInfo, RuntimeEventState, RuntimeInfo, SDLExecutionId}
import org.apache.spark.sql.DataFrame

import java.time.{Duration, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.{util => ju}
import scala.collection.compat.Factory

/**
 * ActionDAGRunState contains all configuration and state of an ActionDAGRun needed to start a recovery run in case of failure.
 */
private[smartdatalake] case class ActionDAGRunState(appConfig: SmartDataLakeBuilderConfig, runId: Int, attemptId: Int, runStartTime: LocalDateTime, attemptStartTime: LocalDateTime
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
private[smartdatalake] case class DataObjectState(dataObjectId: DataObjectId, state: String) {
  def getEntry: (DataObjectId, DataObjectState) = (dataObjectId, this)
}
private[smartdatalake] object ActionDAGRunState {

  import configs.syntax._

  // Json Naming
  implicit def jsonNaming[A]: ConfigKeyNaming[A] = ConfigKeyNaming.lowerCamelCase[A]

  // String converters
  implicit val actionIdStringConverter: StringConverter[ActionId] = StringConverter.fromTry(ActionId, _.id)
  implicit val runtimeEventStateStringConverter: StringConverter[RuntimeEventState] = StringConverter.fromTry(RuntimeEventState.withName, _.toString)
  val localDateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME
  implicit val localDateTimeStringConverter: StringConverter[LocalDateTime] = StringConverter.fromTry(
    LocalDateTime.parse(_, localDateTimeFormatter), localDateTimeFormatter.format
  )
  implicit val durationStringConverter: StringConverter[Duration] = StringConverter.fromTry(Duration.parse, _.toString)

  // json readers
  implicit val mapStringAnyReader: ConfigReader[Map[String,Any]] = {
    val anyReader: ConfigReader[Any] = ConfigReader.fromTry { (c,p) =>
      c.getAnyRef(p) match {
        // only String and Integer needed for now
        case v: String => v
        case v: Integer => v
        case v: java.lang.Long => v
      }
    }
    val javaMapStringAnyConfigReader: ConfigReader[ju.Map[String, Any]] = ConfigReader.javaMapConfigReader(implicitly[StringConverter[String]], anyReader)
    ConfigReader.cbfJMapConfigReader(javaMapStringAnyConfigReader, implicitly[Factory[(String, Any), Map[String, Any]]])
  }
  implicit val seqPartitionValuesReader: ConfigReader[Seq[PartitionValues]] = getSeqReader[PartitionValues]
  // DataFrames are not stored to Json -> always read none
  implicit val dataFrameReader: ConfigReader[Option[DataFrame]] = ConfigReader.successful(None)
  // Prevent build error "cannot derive for `Option[A]`: `A` is not a ConfigReader instance"
  implicit val snowparkDataFrameReader: ConfigReader[Option[com.snowflake.snowpark.DataFrame]] = ConfigReader.successful(None)
  implicit val seqResultRuntimeInfoReader: ConfigReader[Seq[ResultRuntimeInfo]] = getSeqReader[ResultRuntimeInfo]
  implicit val durationReader: ConfigReader[Duration] = ConfigReader.fromStringConfigReader

  // json writers
  implicit val mapStringAnyConfigWriter: ConfigWriter[Map[String,Any]] = {
    val anyWriter: ConfigWriter[Any] = ConfigWriter.from(ConfigValueFactory.fromAnyRef)
    ConfigWriter.mapConfigWriter(StringConverter[String], anyWriter)
  }
  implicit val partitionValuesConfigWriter: ConfigWriter[PartitionValues] = ConfigWriter.derive[PartitionValues]
  // DataFrames are not stored to Json -> always write none
  implicit val dataFrameWriter: ConfigWriter[Option[DataFrame]] = ConfigWriter.from(_ => ConfigValue.Null)
  // Prevent build error "cannot derive for `Option[A]`: `A` is not a ConfigReader instance"
  implicit val snowparkDataFrameWriter: ConfigWriter[Option[com.snowflake.snowpark.DataFrame]] = ConfigWriter.from(_ => ConfigValue.Null)
  implicit val seqResultRuntimeInfoWriter: ConfigWriter[Seq[ResultRuntimeInfo]] = getSeqWriter[ResultRuntimeInfo]
  implicit val durationWriter: ConfigWriter[Duration] = ConfigWriter.stringConfigWriter.contramap(durationStringConverter.toString)

  // write state to Json
  def toJson(actionDAGRunState: ActionDAGRunState): String = {
    val renderOptions = ConfigRenderOptions.defaults().setJson(true).setFormatted(true).setOriginComments(false)
    val writer = ConfigWriter.derive[ActionDAGRunState]
    writer.write(actionDAGRunState).render(renderOptions)
  }

  // read state from json
  def fromJson(stateJson: String): ActionDAGRunState = {
    ConfigFactory.parseString(stateJson).extract[ActionDAGRunState] match {
      case Success(value) => value
      case Failure(e) => throw new ConfigurationException(s"Unable to parse state from json: ${e.messages.mkString(",")}")
      case _ => throw new Exception("Unmatched case, should never happen.")
    }
  }

  // Helpers
  private def getSeqReader[A: ConfigReader]: ConfigReader[Seq[A]] = {
    ConfigReader.cbfJListConfigReader(ConfigReader.javaListConfigReader[A], implicitly[Factory[A, Seq[A]]])
  }
  private def getSeqWriter[A: ConfigWriter]: ConfigWriter[Seq[A]] = {
    ConfigWriter.iterableConfigWriter
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