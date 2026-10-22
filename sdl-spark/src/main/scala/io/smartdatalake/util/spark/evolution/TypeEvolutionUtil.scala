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
import org.apache.spark.sql.types._
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
   * Returns the wider of two simple Spark data types, e.g. a data type that can hold the values of both data types,
   * or None if there is no such data type.
   *
   * Rules:
   * - integral and floating point numbers are widened according to Byte < Short < Int < Long < Double and Float < Double.
   *   Int or Long combined with Float results in Double.
   * - decimals are widened to hold the integral digits and the scale of both. If this exceeds the maximum precision,
   *   the integral digits are kept and the scale is reduced.
   * - integral numbers combined with decimals are handled as decimals with scale 0.
   * - decimals with precision <= 16 combined with Float or Double result in Double.
   * - numbers combined with strings result in String.
   */
  def widerSimpleType(left: DataType, right: DataType): Option[DataType] = {
    val floatingPointPrecedence: Map[DataType, Int] = Map(FloatType -> 0, DoubleType -> 1)
    (left, right) match {
      case _ if left == right => Some(left)
      case (l: DecimalType, r: DecimalType) => Some(widerDecimalType(l, r))
      case (l: DecimalType, r) if integralDecimalType(r).isDefined => Some(widerDecimalType(l, integralDecimalType(r).get))
      case (l, r: DecimalType) if integralDecimalType(l).isDefined => Some(widerDecimalType(integralDecimalType(l).get, r))
      case (l: DecimalType, _: FloatType | _: DoubleType) if l.precision <= 16 => Some(DoubleType)
      case (_: FloatType | _: DoubleType, r: DecimalType) if r.precision <= 16 => Some(DoubleType)
      case (l, r) if integralPrecedence.contains(l) && integralPrecedence.contains(r) =>
        Some(if (integralPrecedence(l) >= integralPrecedence(r)) l else r)
      case (l, r) if floatingPointPrecedence.contains(l) && floatingPointPrecedence.contains(r) =>
        Some(if (floatingPointPrecedence(l) >= floatingPointPrecedence(r)) l else r)
      case (_: ByteType | _: ShortType, _: FloatType) | (_: FloatType, _: ByteType | _: ShortType) => Some(FloatType)
      case (l, _: FloatType | _: DoubleType) if integralPrecedence.contains(l) => Some(DoubleType)
      case (_: FloatType | _: DoubleType, r) if integralPrecedence.contains(r) => Some(DoubleType)
      case (l: StringType, _: NumericType) => Some(l)
      case (_: NumericType, r: StringType) => Some(r)
      case _ => None
    }
  }

  private val integralPrecedence: Map[DataType, Int] = Map(ByteType -> 0, ShortType -> 1, IntegerType -> 2, LongType -> 3)

  /**
   * Decimal type which can hold all values of an integral type.
   */
  def integralDecimalType(dataType: DataType): Option[DecimalType] = dataType match {
    case ByteType => Some(DecimalType(3, 0))
    case ShortType => Some(DecimalType(5, 0))
    case IntegerType => Some(DecimalType(10, 0))
    case LongType => Some(DecimalType(20, 0))
    case _ => None
  }

  private def widerDecimalType(l: DecimalType, r: DecimalType): DecimalType = {
    val integralDigits = math.max(l.precision - l.scale, r.precision - r.scale)
    val scale = math.min(math.max(l.scale, r.scale), math.max(DecimalType.MAX_PRECISION - integralDigits, 0))
    DecimalType(math.min(integralDigits + scale, DecimalType.MAX_PRECISION), scale)
  }

  /**
   * Checks if a Spark DataType is castable to another
   */
  def isSimpleTypeCastable(left: DataType, right: DataType): Boolean = {
    Try(ValueProjector.getSimpleTypeConverter(left, right, Seq())).isSuccess
  }

}
