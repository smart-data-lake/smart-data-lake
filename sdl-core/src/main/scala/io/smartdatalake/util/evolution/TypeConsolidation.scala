/*
 * sdl-core - Build your data lake the smart way.
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
package io.smartdatalake.util.evolution

import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe._

/**
 * Implementation of schema evolution for complex types as struct, array and map.
 */
private[smartdatalake] object TypeConsolidation extends SmartDataLakeLogger {

  /**
   * Creates a consolidated DataType of given old and new DataType's. Handles new columns and deleted columns.
   *
   * @param leftType old DataType
   * @param rightType new DataType
   * @param ignoreOldDeletedColumns if true, remove no longer existing columns
   * @param path expression path for logging purposes. Can be filled with column name for better traceability.
   * @return consolidated DataType
   */
  def consolidateType(leftType: GenericDataType, rightType: GenericDataType, ignoreOldDeletedColumns: Boolean = true, path: Seq[String] = Seq()): GenericDataType = {
    val functions = DataFrameSubFeed.getFunctions(leftType.subFeedType)
    (leftType, rightType) match {
      case (leftType: GenericDataType with GenericStructDataType, rightType: GenericDataType with GenericStructDataType) => // struct type -> recursion
        consolidateStructType(leftType, rightType, ignoreOldDeletedColumns, path)
      case (leftType: GenericDataType with GenericArrayDataType, rightType: GenericDataType with GenericArrayDataType) => // array type -> recursion on element type
        functions.arrayType(consolidateType(leftType.elementDataType, rightType.elementDataType, ignoreOldDeletedColumns, path))
      case (leftType: GenericDataType with GenericMapDataType, rightType: GenericDataType with GenericMapDataType) => // map type -> consolidate key + consolidate value
        val consolidatedKeyType = consolidateType(leftType.keyDataType, rightType.keyDataType, ignoreOldDeletedColumns, path :+ "key")
        val consolidatedValueType = consolidateType(leftType.valueDataType, rightType.valueDataType, ignoreOldDeletedColumns, path :+ "value")
        functions.mapType(consolidatedKeyType, consolidatedValueType)
      case (leftType, rightType) if leftType.isSameType(rightType) => // data type equal
        rightType
      case (leftType: GenericDataType with GenericSimpleDataType, rightType: GenericDataType with GenericSimpleDataType) => // assume that it is castable
        rightType
      case _ => // otherwise not supported
        throw SchemaEvolutionException(s"""schema evolution from $leftType to $rightType not supported (field ${path.mkString(".")}""")
    }
  }

  def consolidateStructType(leftSchema: GenericDataType with GenericStructDataType, rightSchema: GenericDataType with GenericStructDataType, ignoreOldDeletedColumns: Boolean = false, path: Seq[String] = Seq()): GenericDataType with GenericStructDataType = {
    val functions = DataFrameSubFeed.getFunctions(leftSchema.subFeedType)
    val deletedColumns = leftSchema.fieldNames.diff(rightSchema.fieldNames)
    val tgtFields = (rightSchema.fieldNames ++ deletedColumns).flatMap {
      column =>
        val leftFieldOpt = leftSchema.fields.find(_.name == column)
        val rightFieldOpt = rightSchema.fields.find(_.name == column)
        (leftFieldOpt,rightFieldOpt) match {
          case (None, Some(rightField)) => // add new fields
            Some(rightField)
          case (Some(leftField), None) => // add old fields if desired
            if (ignoreOldDeletedColumns) None else Some(leftField)
          case (Some(leftField), Some(rightField)) =>
            Some(functions.field(column, consolidateType(leftField.dataType, rightField.dataType, ignoreOldDeletedColumns, path :+ column), leftField.nullable || rightField.nullable))
          case _ => throw new IllegalStateException()
        }
    }
    functions.structType(tgtFields)
  }
}
