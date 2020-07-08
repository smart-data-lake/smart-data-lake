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
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex
import scala.reflect.runtime.universe.TypeTag

/**
 * Utils to misuse spark sql expressions as expression language to substitute tokens with values from case class instances.
 */
private[smartdatalake] object SparkExpressionUtil {

  val tokenStartChar = "%"
  val tokenExpressionRegex = s"""$tokenStartChar\{(.*?)\}""".r

  /**
   * Substitutes all tokens in a string by the expression defined by the token evaluated against the give case class instance.
   * Token syntax: "${<spark sql expression>}
   * @param id object id for logging
   * @param configName config name for logging
   * @param str String with tokens to replace
   * @param data Case class instance with data to be used as replacement
   * @param onlySyntaxCheck If true only expression syntax is checked. Use for validation only.
   */
  def substitute[T <: Product : TypeTag](id: ConfigObjectId, configName: Option[String], str: String, data: T, onlySyntaxCheck: Boolean = false)(implicit session: SparkSession): String = {
    import org.apache.spark.sql.functions.expr
    import org.apache.spark.sql.types.StringType
    import session.implicits._
    val dsData = Seq(data).toDS
    val substituter = (regMatch: Regex.Match) => {
      val expression = regMatch.group(1)
      val dsValue = Try {
        dsData.select(expr(expression).cast(StringType).as("value")).as[Option[String]]
      } match {
        case Success(v) => v
        case Failure(e) => throw ConfigurationException(s"($id) spark expression substitution for '$expression' not possible", configName, e)
      }
      if (!onlySyntaxCheck) Try {
        dsValue.head
      } match {
        case Success(v) => v.getOrElse(throw new IllegalStateException(s"($id) spark expression substitution for '$expression' and config $configName not defined by $data"))
        case Failure(e) => throw new IllegalStateException(s"($id) spark expression substitution for '$expression' and config $configName failed", e)
      }
      else s"#expression#" // use dummy replacement if onlySyntaxCheck=true
    }
    tokenExpressionRegex.replaceAllIn(str, substituter)
  }

}

case class DefaultExpressionData(feed: String, application: String, runId: Int, attemptId: Int, referenceTimestamp: Option[Timestamp] = None, partitionValues: Seq[Map[String,String]])

