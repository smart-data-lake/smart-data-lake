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
package io.smartdatalake.util.spark

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.definitions.Environment
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.StructType

object SparkExpressionUtil {

  def resolveExpression(exprStr: String, schema: StructType): Expression = {
    resolveExpression(expr(exprStr).expr, schema)
  }

  def resolveExpression(exprCol: Expression, schema: StructType): Expression = {
    // invoke dynamically to allow different implementations
    val resolveExpressionMethod = Environment.expressionEvaluatorFactory().getClass.getMethod("resolveExpression", classOf[Expression], classOf[StructType])
    resolveExpressionMethod.invoke(Environment.expressionEvaluatorFactory(), exprCol, schema).asInstanceOf[Expression]
  }

  def registerSparkUdf(name: String, udf: UserDefinedFunction): AnyRef = {
    // invoke dynamic
    val applyUdfMethod = Environment.expressionEvaluatorFactory().getClass.getMethods.find(_.getName == "registerSparkUdf")
    applyUdfMethod.map(_.invoke(Environment.expressionEvaluatorFactory(), name, udf))
      .getOrElse(throw new ConfigurationException(
        s"Could not register Spark UDF '$name': ${Environment.expressionEvaluatorFactory().getClass.getSimpleName} has no method 'registerSparkUdf'",
        Some("global.sparkUDFs")
      ))
  }
}
