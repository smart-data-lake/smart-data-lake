/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.spark

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.custom.ExpressionEvaluator
import org.apache.spark.sql.functions.expr

import java.sql.Timestamp
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.matching.Regex

/**
 * Utils to misuse spark sql expressions as expression language to substitute tokens with values from case class instances.
 */
private[smartdatalake] object SparkExpressionUtil {

  private val tokenStartChar = "%"
  private val tokenExpressionRegex = (tokenStartChar + """\{(.*?)\}""").r

  /**
   * Substitutes all tokens in a string by the expression defined by the token evaluated against the given case class instance.
   * Token syntax: "%{<spark sql expression>}
   * @param id object id for logging
   * @param configName config name for logging
   * @param str String with tokens to replace
   * @param data Case class instance with data to be used as replacement
   */
  def substitute[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], str: String, data: T): String = {
    val substituter = (regMatch: Regex.Match) => {
      val expression = regMatch.group(1)
      val value = evaluateString[T](id, configName, expression, data)
      value.getOrElse {
        throw new IllegalStateException(s"($id) spark expression evaluation for '$expression'${getConfigNameMsg(configName)} not defined by $data")
      }
    }
    tokenExpressionRegex.replaceAllIn(str, substituter)
  }

  /**
   * Evaluate an expression with boolean return type against a given case class instance
   * @param id id of the config object for meaningful exception text
   * @param configName optional configuration name for meaningful exception text
   * @param expression expression to be evaluated
   * @param data case class instance
   * @tparam T class of object the expression should be evaluated on
   */
  def evaluateBoolean[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], expression: String, data: T, syntaxCheckOnly: Boolean = false): Boolean =
    evaluate[T, Boolean](id, configName, expression, data)
      .getOrElse(false)

  /**
   * Evaluate an expression with string return type against a given case class instance
   * @param id id of the config object for meaningful exception text
   * @param configName optional configuration name for meaningful exception text
   * @param expression expression to be evaluated
   * @param data case class instance
   * @tparam T class of object the expression should be evaluated on
   */
  def evaluateString[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], expression: String, data: T): Option[String] =
    evaluate[T, Any](id, configName, expression, data)
      .map(_.toString)

  /**
   * Evaluate an expression against a given case class instance
   * @param id id of the config object for meaningful exception text
   * @param configName optional configuration name for meaningful exception text
   * @param expression expression to be evaluated as String
   * @param data case class instance
   * @tparam T class of object the expression should be evaluated on
   * @tparam R class of expressions expected return type
   */
  def evaluate[T <: Product : TypeTag, R : TypeTag : ClassTag](id: ConfigObjectId, configName: Option[String], expression: String, data: T): Option[R] = {
    evaluate[T,R](id, configName, expr(expression), data)
  }

  /**
   * Evaluate an expression against a given case class instance
   * @param id id of the config object for meaningful exception text
   * @param configName optional configuration name for meaningful exception text
   * @param expression expression to be evaluated as Spark column
   * @param data case class instance
   * @tparam T class of object the expression should be evaluated on
   * @tparam R class of expressions expected return type
   */
  def evaluate[T <: Product : TypeTag, R : TypeTag : ClassTag](id: ConfigObjectId, configName: Option[String], expression: Column, data: T): Option[R] = {
    try {
      val evaluator = new ExpressionEvaluator[T,R](expression)
      Option(evaluator(data))
    } catch {
      case e: Exception =>
        throw ConfigurationException(s"($id) spark expression evaluation for '$expression'${getConfigNameMsg(configName)} failed: ${e.getMessage}", configName, e)
    }
  }

  /**
   * Evaluate an expression against each entry of a list of case class instances
   * @param id id of the config object for meaningful exception text
   * @param configName optional configuration name for meaningful exception text
   * @param expression expression to be evaluated
   * @param data a list of case class instances
   * @tparam T class of object the expression should be evaluated on
   * @tparam R class of expressions expected return type
   */
  def evaluateSeq[T <: Product : TypeTag, R : TypeTag : ClassTag](id: ConfigObjectId, configName: Option[String], expression: String, data: Seq[T]): Seq[(T,Option[R])] = {
    try {
      val evaluator = new ExpressionEvaluator[T,R](expr(expression))
      data.map(d => (d, Option(evaluator(d))))
    } catch {
      case e: Exception =>
        throw ConfigurationException(s"($id) spark expression evaluation for '$expression'${getConfigNameMsg(configName)} failed: ${e.getMessage}", configName, e)
    }
  }

  /**
   * Check syntax of an expression against a given case class
   * @param id id of the config object for meaningful exception text
   * @param configName optional configuration name for meaningful exception text
   * @param expression expression to be evaluated
   * @tparam T class of object the expression should be evaluated on
   * @tparam R class of expressions expected return type
   */
  def syntaxCheck[T <: Product : TypeTag, R : TypeTag : ClassTag](id: ConfigObjectId, configName: Option[String], expression: String): Unit = {
    try {
      new ExpressionEvaluator[T,R](expr(expression))
    } catch {
      case e: Exception =>
        throw ConfigurationException(s"($id) spark expression syntax check for '$expression'${getConfigNameMsg(configName)} failed: ${e.getMessage}", configName, e)
    }
  }

  /**
   * Substitute tokens with value from options
   */
  def substituteOptions(id: ConfigObjectId, configName: Option[String], str: String, options: Map[String,String]): String = {
    val substituter = (regMatch: Regex.Match) => {
      val key = regMatch.group(1)
      options.getOrElse(key, {
        throw ConfigurationException(s"($id) key '$key' not found in options${getConfigNameMsg(configName)}")
      })
    }
    tokenExpressionRegex.replaceAllIn(str, substituter)
  }

  private def getConfigNameMsg(configName: Option[String]) = configName.map(" from config "+_).getOrElse("")
}

/**
 * DefaultExpressionData presents information from the context of the SDLB job for evaluation by Spark expressions in various places of the configuration.
 */
case class DefaultExpressionData( feed: String, application: String, runId: Int, attemptId: Int, executionPhase:String, referenceTimestamp: Option[Timestamp]
                                  , runStartTime: Timestamp, attemptStartTime: Timestamp, partitionValues: Seq[Map[String,String]])
object DefaultExpressionData {
  def from(context: ActionPipelineContext, partitionValues: Seq[PartitionValues]): DefaultExpressionData = {
    DefaultExpressionData(context.feed, context.application, context.executionId.runId, context.executionId.attemptId, context.phase.toString
      , context.referenceTimestamp.map(Timestamp.valueOf), Timestamp.valueOf(context.runStartTime)
      , Timestamp.valueOf(context.attemptStartTime), partitionValues.map(_.elements.mapValues(_.toString).toMap))
  }
}

private[smartdatalake] case class ExpressionEvaluationException(message: String, configurationPath: Option[String] = None, throwable: Throwable = null) extends RuntimeException(message, throwable)
