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
package io.smartdatalake.util.evolution

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

/**
 * Implementation of schema evolution for complex types as struct, array and map.
 */
private[smartdatalake] object ComplexTypeEvolution extends SmartDataLakeLogger {

  /**
   * Conversion of [[Row]]'s from one schema to another.
   *
   * @param rows input rows
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
   * Creates a Spark udf to convert a [[org.apache.spark.sql.Column]] from one schema to another.
   *
   * @param srcType DataType of the column to be converted
   * @param tgtType target DataType
   * @return udf to convert a [[org.apache.spark.sql.Column]] to the target DataType
   * @throws SchemaEvolutionException if conversion is not possible
   */
  def schemaEvolutionUdf(srcType: DataType, tgtType: DataType): UserDefinedFunction = {
    val projector = ValueProjector.getProjection(srcType, tgtType, Seq())
    udf((row: Any) => projector.getWithCast(row), tgtType)
  }

  /**
   * Creates a consolidated DataType of given old and new DataType's. Handles new columns and deleted columns.
   * To be used to create tgtSchema/tgtType for methods [[schemaEvolution]] and [[schemaEvolutionUdf]].
   *
   * @param leftType old DataType
   * @param rightType new DataType
   * @param ignoreOldDeletedColumns if true, remove no longer existing columns
   * @param path expression path for logging purposes. Can be filled with column name for better traceability.
   * @return consolidated DataType
   */
  def consolidateType(leftType: DataType, rightType: DataType, ignoreOldDeletedColumns: Boolean = true, path: Seq[String] = Seq()): DataType = {
    (leftType, rightType) match {
      case (leftType: StructType, rightType: StructType) => // struct type -> recursion
        consolidateStructType(leftType, rightType, ignoreOldDeletedColumns, path)
      case (leftType: ArrayType, rightType: ArrayType) => // array type -> recursion on element type
        ArrayType(consolidateType(leftType.elementType, rightType.elementType, ignoreOldDeletedColumns, path))
      case (leftType: MapType, rightType: MapType) => // array type -> recursion on element type
        val consolidatedKeyType = consolidateType(leftType.keyType, rightType.keyType, ignoreOldDeletedColumns, path :+ "key")
        val consolidatedValueType = consolidateType(leftType.valueType, rightType.valueType, ignoreOldDeletedColumns, path :+ "value")
        MapType(consolidatedKeyType, consolidatedValueType)
      case (leftType, rightType) if leftType.simpleString == rightType.simpleString => // data type equal
        rightType
      case (leftType, rightType) if SchemaEvolution.isSimpleTypeCastable(leftType, rightType) => // type changed and castable
        rightType
      case _ => // otherwise not supported
        throw SchemaEvolutionException(s"""schema evolution from $leftType to $rightType not supported (field ${path.mkString(".")}""")
    }
  }

  private def consolidateStructType(leftSchema: StructType, rightSchema: StructType, ignoreOldDeletedColumns: Boolean = false, path: Seq[String] = Seq()): StructType = {
    val deletedColumns = leftSchema.map(_.name).diff(rightSchema.map(_.name))
    val tgtFields = (rightSchema.map(_.name) ++ deletedColumns).flatMap {
      column =>
        val leftFieldOpt = leftSchema.find( _.name == column)
        val rightFieldOpt = rightSchema.find( _.name == column)
        (leftFieldOpt,rightFieldOpt) match {
          case (None, Some(rightField)) => // add new fields
            Some(rightField)
          case (Some(leftField), None) => // add old fields if desired
            if (ignoreOldDeletedColumns) None else Some(leftField)
          case (Some(leftField), Some(rightField)) =>
            Some(StructField(column, consolidateType(leftField.dataType,rightField.dataType, ignoreOldDeletedColumns, path :+ column), leftField.nullable || rightField.nullable))
          case _ => throw new IllegalStateException()
        }
    }
    StructType(tgtFields)
  }
}
