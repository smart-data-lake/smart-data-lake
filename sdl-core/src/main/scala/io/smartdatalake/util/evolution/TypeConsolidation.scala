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
package io.smartdatalake.util.evolution

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SchemaUtil.{findByName, normalizeColName}
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.dataframe._

/**
 * Implementation of schema evolution for complex types as struct, array and map.
 */
object TypeConsolidation {

  /**
   * Creates a consolidated DataType of given old and new DataType's. Handles new columns and deleted columns.
   *
   * @param leftType old DataType
   * @param rightType new DataType
   * @param ignoreOldDeletedColumns if true, remove no longer existing columns
   * @param path expression path for logging purposes. Can be filled with column name for better traceability.
   * @param caseSensitive if true, names of nested fields are compared case-sensitive.
   *                      Note that the conversion of the values by the engine uses [[Environment.caseSensitive]].
   * @return consolidated DataType
   */
  def consolidateType(leftType: GenericDataType, rightType: GenericDataType, ignoreOldDeletedColumns: Boolean = true, path: Seq[String] = Seq(), caseSensitive: Boolean = Environment.caseSensitive): GenericDataType = {
    val functions = DataFrameSubFeed.getFunctions(leftType.subFeedType)
    (leftType, rightType) match {
      case (leftType: GenericDataType with GenericStructDataType, rightType: GenericDataType with GenericStructDataType) => // struct type -> recursion
        consolidateStructType(leftType, rightType, ignoreOldDeletedColumns, path, caseSensitive)
      case (leftType: GenericDataType with GenericArrayDataType, rightType: GenericDataType with GenericArrayDataType) => // array type -> recursion on element type
        functions.arrayType(consolidateType(leftType.elementDataType, rightType.elementDataType, ignoreOldDeletedColumns, path, caseSensitive))
      case (leftType: GenericDataType with GenericMapDataType, rightType: GenericDataType with GenericMapDataType) => // map type -> consolidate key + consolidate value
        val consolidatedKeyType = consolidateType(leftType.keyDataType, rightType.keyDataType, ignoreOldDeletedColumns, path :+ "key", caseSensitive)
        val consolidatedValueType = consolidateType(leftType.valueDataType, rightType.valueDataType, ignoreOldDeletedColumns, path :+ "value", caseSensitive)
        functions.mapType(consolidatedKeyType, consolidatedValueType)
      case (leftType, rightType) if leftType.isSameType(rightType) => // data type equal
        rightType
      case (leftType: GenericDataType with GenericSimpleDataType, rightType: GenericDataType with GenericSimpleDataType) => // assume that it is castable
        rightType
      case _ => // otherwise not supported
        throw SchemaEvolutionException(s"schema evolution from $leftType to $rightType not supported (field ${path.mkString(".")})")
    }
  }

  /**
   * Creates a consolidated struct type of given old and new struct type.
   * Fields are ordered as in the new struct type, followed by deleted fields if they are kept.
   * Name and metadata of a field existing in both struct types are taken from the new struct type.
   */
  def consolidateStructType(leftSchema: GenericDataType with GenericStructDataType, rightSchema: GenericDataType with GenericStructDataType, ignoreOldDeletedColumns: Boolean = true, path: Seq[String] = Seq(), caseSensitive: Boolean = Environment.caseSensitive): GenericDataType with GenericStructDataType = {
    val functions = DataFrameSubFeed.getFunctions(leftSchema.subFeedType)
    val rightFieldNames = rightSchema.fields.map(f => normalizeColName(f.name, caseSensitive)).toSet
    val deletedFields = leftSchema.fields.filterNot(f => rightFieldNames.contains(normalizeColName(f.name, caseSensitive)))
    val tgtFields = rightSchema.fields.map { rightField =>
      findByName[GenericField](leftSchema.fields, rightField.name, _.name, caseSensitive) match {
        case None => // new field
          rightField
        case Some(leftField) =>
          val tgtType = consolidateType(leftField.dataType, rightField.dataType, ignoreOldDeletedColumns, path :+ rightField.name, caseSensitive)
          rightField.withDataType(tgtType, leftField.nullable || rightField.nullable)
      }
    } ++ (if (ignoreOldDeletedColumns) Seq() else deletedFields)
    functions.structType(tgtFields)
  }
}
