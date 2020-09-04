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

package io.smartdatalake.util.misc

import java.sql.Timestamp

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.reflect.runtime.universe.TypeTag
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

/**
 * Utils to misuse spark sql expressions as expression language to substitute tokens with values from case class instances.
 */
private[smartdatalake] object SparkExpressionUtil {

  val tokenStartChar = "%"
  val tokenExpressionRegex = (tokenStartChar + """\{(.*?)\}""").r

  /**
   * Substitutes all tokens in a string by the expression defined by the token evaluated against the given case class instance.
   * Token syntax: "%{<spark sql expression>}
   * @param id object id for logging
   * @param configName config name for logging
   * @param str String with tokens to replace
   * @param data Case class instance with data to be used as replacement
   * @param onlySyntaxCheck If true only expression syntax is checked. Use for validation only.
   */
  def substitute[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], str: String, data: T, onlySyntaxCheck: Boolean = false)(implicit session: SparkSession): String = {
    val substituter = (regMatch: Regex.Match) => {
      val expression = regMatch.group(1)
      val value = evaluateString(id, configName, expression, data, onlySyntaxCheck)
      if (!onlySyntaxCheck) value.getOrElse(throw new IllegalStateException(s"($id) spark expression evaluation for '$expression' and config $configName not defined by $data"))
      else s"#expression#" // use dummy replacement if onlySyntaxCheck=true
    }
    tokenExpressionRegex.replaceAllIn(str, substituter)
  }

  def evaluateBoolean[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], expression: String, data: T, onlySyntaxCheck: Boolean = false)(implicit session: SparkSession): Boolean = {
    import session.implicits._
    val dsResult = createDataset[T,Boolean](id, configName, expression, data)
    if (!onlySyntaxCheck) {
      Try {
        dsResult.head
      } match {
        case Success(v) => v.getOrElse(throw new IllegalStateException(s"($id) spark expression evaluation for '$expression' and config $configName not defined by $data"))
        case Failure(e) => throw new IllegalStateException(s"($id) spark expression evaluation for '$expression' and config $configName failed", e)
      }
    } else false
  }

  def evaluateString[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], expression: String, data: T, onlySyntaxCheck: Boolean = false)(implicit session: SparkSession): Option[String] = {
    import session.implicits._
    val dsResult = createDataset[T,String](id, configName, expression, data)
    if (!onlySyntaxCheck) {
      Try {
        dsResult.head
      } match {
        case Success(v) => v
        case Failure(e) => throw new IllegalStateException(s"($id) spark expression evaluation for '$expression' and config $configName failed", e)
      }
    } else None
  }

  def evaluateAny[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], expression: String, data: T, onlySyntaxCheck: Boolean = false)(implicit session: SparkSession): Option[Any] = {
    import session.implicits._
    val dsData = Seq(data).toDS
    val dfResult = Try {
      dsData.select(expr(expression))
    } match {
      case Success(v) => v
      case Failure(e) => throw ConfigurationException(s"($id) spark expression evaluation for '$expression' and config $configName not possible", configName, e)
    }
    if (!onlySyntaxCheck) {
      Try {
        val result = dfResult.head
        if (!result.isNullAt(0)) Some(dfResult.head.get(0))
        else None
      } match {
        case Success(v) => v
        case Failure(e) => throw new IllegalStateException(s"($id) spark expression evaluation for '$expression' and config $configName failed", e)
      }
    } else None
  }

  private def createDataset[T <: Product : TypeTag, R](id: ConfigObjectId, configName: Option[String], expression: String, data: T)(implicit session: SparkSession, encoder: Encoder[Option[R]]): Dataset[Option[R]] = {
    import org.apache.spark.sql.functions.expr
    import session.implicits._
    val dsData = Seq(data).toDS
    Try {
      dsData.select(expr(expression).cast(encoder.schema.head.dataType)).as[Option[R]]
    } match {
      case Success(v) => v
      case Failure(e) => throw ConfigurationException(s"($id) spark expression evaluation for '$expression' and config $configName not possible", configName, e)
    }
  }

  /**
   * Substitute tokens with value from options
   */
  def substituteOptions(id: ConfigObjectId, configName: Option[String], str: String, options: Map[String,String]): String = {
    val substituter = (regMatch: Regex.Match) => {
      val key = regMatch.group(1)
      options.getOrElse(key, throw ConfigurationException(s"($id) key '$key' not found in options for config $configName"))
    }
    tokenExpressionRegex.replaceAllIn(str, substituter)
  }
}

case class DefaultExpressionData( feed: String, application: String, runId: Int, attemptId: Int, executionPhase:String, referenceTimestamp: Option[Timestamp]
                                  , runStartTime: Timestamp, attemptStartTime: Timestamp, partitionValues: Seq[Map[String,String]])
object DefaultExpressionData {
  def from(context: ActionPipelineContext, partitionValues: Seq[PartitionValues]): DefaultExpressionData = {
    DefaultExpressionData(context.feed, context.application, context.runId, context.attemptId, context.phase.toString
      , context.referenceTimestamp.map(Timestamp.valueOf), Timestamp.valueOf(context.runStartTime)
      , Timestamp.valueOf(context.attemptStartTime), partitionValues.map(_.elements.mapValues(_.toString)))
  }
}
