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

package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericColumn}

/**
 * Definition of row-level constraint to validate.
 *
 * @param name name of the constraint
 * @param description optional detailed description of the constraint
 * @param expression SQL expression to evaluate on every row. The expressions return value should be a boolean.
 *                   If it evaluates to true the constraint is validated successfully, otherwise it will throw an exception.
 * @param errorMsgCols Optional list of column names to add to error message.
 *                     Note that primary key colums are always included.
 *                     If there is no primary key defined, by default all columns with simple datatype are included in the error message.
 */
case class Constraint(name: String, description: Option[String] = None, expression: String, errorMsgCols: Seq[String] = Seq()) {
  def getExpressionColumn(dataObjectId: DataObjectId)(implicit functions: DataFrameFunctions): GenericColumn = {
    try {
      functions.expr(expression)
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Constraint '$name': cannot parse SQL expression '$expression'", Some(s"constraints.$name.expression"), e)
    }
  }
  def getValidationErrorColumn(dataObjectId: DataObjectId, pkColNames: Option[Seq[String]], simpleTypeColNames: Seq[String])(implicit functions: DataFrameFunctions): GenericColumn = {
    val expressionCol = getExpressionColumn(dataObjectId)
    import functions._
    try {
      val traceColNames = pkColNames.map(_ ++ errorMsgCols)
        .orElse(if(errorMsgCols.nonEmpty) Some(errorMsgCols) else None)
        .getOrElse(simpleTypeColNames)
      def mkString(cols: Seq[GenericColumn]): GenericColumn = concat(cols.head.cast(stringType) +: cols.tail.flatMap(c => Seq(lit(" "), c.cast(stringType))):_*)
      val traceCol = mkString(traceColNames.map(c => concat(lit(s"$c="), col(c))))
      when(not(expressionCol), concat(lit(s"($dataObjectId) Constraint '$name' failed for record: "), traceCol))
    } catch {
      case e: Exception => throw new ConfigurationException(s"($dataObjectId) Constraint '$name': cannot create validation error column", Some(s"constraints.$name"), e)
    }
  }
  def getValidationExceptionColumn(dataObjectId: DataObjectId, pkCols: Option[Seq[String]], simpleTypeCols: Seq[String])(implicit functions: DataFrameFunctions): GenericColumn = {
    import functions._
    when(not(getExpressionColumn(dataObjectId)), raise_error(getValidationErrorColumn(dataObjectId, pkCols, simpleTypeCols)))
  }
}

case class ConstraintValidationException(msg: String) extends Exception(msg)