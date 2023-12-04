/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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

package com.snowflake.snowpark.custom

import com.snowflake.snowpark.DataFrame
import com.snowflake.snowpark.types.{DataType, NumericType, StructType}

object SnowparkUtils {
  def isNumeric(dataType: DataType) = dataType.isInstanceOf[NumericType]
  def showString(df: DataFrame, numRows: Int = 10, width: Int = 200): String = df.showString(numRows, width)
  def explainString(df: DataFrame): String = df.explainString
  def schemaTreeString(schema: StructType, level: Int = Int.MaxValue): String = schema.treeString(level)
}
