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
package org.apache.spark.sql.classic

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Wrap a catalyst Expression into a Column.
 *
 * Spark 4 replaced `new Column(expression)` with ColumnNodes. The remaining bridge, `ExpressionUtils.column`,
 * is private to Spark, so this helper lives in Sparks package. The other direction is available through
 * `ColumnConversions`.
 */
object SdlColumnExpression {
  def toColumn(expression: Expression): Column = ExpressionUtils.column(expression)
}
