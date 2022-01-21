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

package io.smartdatalake.definitions

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.ConfigObjectId
import io.smartdatalake.util.spark.SparkExpressionUtil

import scala.reflect.runtime.universe.TypeTag

/**
 * Trait defining basics for a Spark SQL condition with description.
 */
private[smartdatalake] trait ConditionBase {
  def expression: String
  def description: Option[String]

  private[smartdatalake] def syntaxCheck[T<:Product:TypeTag](id: ConfigObjectId, configName: Option[String]): Unit = {
    SparkExpressionUtil.syntaxCheck[T,Boolean](id, configName, expression)
  }

  private[smartdatalake] def evaluate[T<:Product:TypeTag](id: ConfigObjectId, configName: Option[String], data: T): Boolean = {
    SparkExpressionUtil.evaluateBoolean[T](id, configName, expression, data)
  }
}

/**
 * Definition of a Spark SQL condition with description.
 * This is used for example to define failConditions of [[PartitionDiffMode]].
 *
 * @param expression Condition formulated as Spark SQL. The attributes available are dependent on the context.
 * @param description A textual description of the condition to be shown in error messages.
 */
case class Condition(override val expression: String, override val description: Option[String] = None) extends ConditionBase