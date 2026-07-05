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
package io.smartdatalake.util.spark.evolution

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.evolution.SchemaEvolutionException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.spark.SparkDataType
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.custom.UnsafeUnaryUdf
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, Row}

import scala.util.Try

object TypeEvolutionUtil extends SmartDataLakeLogger {

  /**
   * Conversion of Spark [[Row]]'s from one schema to another.
   *
   * @param rows      input rows
   * @param srcSchema schema of the input rows
   * @param tgtSchema target schema
   * @return rows converted to target schema
   * @throws SchemaEvolutionException if conversion is not possible
   */
  def schemaEvolution(rows: Iterator[Row], srcSchema: StructType, tgtSchema: StructType): Iterator[Row] = {
    // initialize schema projection
    val projector = StructTypeValueProjector(srcSchema, tgtSchema, Seq())
    logger.info(s"projection: $projector")
    // apply projection to all rows
    rows.map(row => projector.get(row))
  }

  /**
   * Creates a Spark udf to convert an Expression from one schema to another.
   *
   * @param srcType DataType of the column to be converted
   * @param tgtType target DataType
   * @return udf to convert an Expression to the target DataType
   * @throws SchemaEvolutionException if conversion is not possible
   */
  def schemaEvolutionUdf(srcType: StructType, tgtType: StructType): Expression => Expression = {
    val projector = ValueProjector.getProjection(srcType, tgtType, Seq())
    UnsafeUnaryUdf((row: Any) => projector.getWithCast(row), srcType, tgtType)
  }

  def isSameType(t1: DataType, t2: DataType): Boolean = {
    val t1Clean = SparkDataType(t1).removeMetadata.inner
    val t2Clean = SparkDataType(t2).removeMetadata.inner
    if (Environment.caseSensitive) DataType.equalsIgnoreNullability(t1Clean, t2Clean)
    else DataType.equalsIgnoreCaseAndNullability(t1Clean, t2Clean)
  }


  /**
   * Checks if a Spark DataType is castable to another
   */
  def isSimpleTypeCastable(left: DataType, right: DataType): Boolean = {
    Try(ValueProjector.getSimpleTypeConverter(left, right, Seq())).isSuccess
  }

}
