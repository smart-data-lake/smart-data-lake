/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.util.misc

import io.smartdatalake.workflow.dataframe._
import org.apache.spark.sql.internal.SQLConf

object SchemaUtil {

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight`: `Set(schemaLeft)` \ `Set(schemaRight)`.
   *
   * @param schemaLeft schema used as minuend.
   * @param schemaRight schema used as subtrahend.
   * @param ignoreNullable if `true`, columns that only differ in their `nullable` property are considered equal.
   * @return the set of columns contained in `schemaRight` but not in `schemaLeft`.
   */
  def schemaDiff(schemaLeft: GenericSchema, schemaRight: GenericSchema, ignoreNullable: Boolean = false, caseSensitive: Boolean = false, deep: Boolean = false): Set[GenericField] = {
    if (deep) {
      deepPartialMatchDiffFields(schemaLeft.fields, schemaRight.fields, ignoreNullable, caseSensitive)
    } else {
      val left = prepareSchemaForDiff(schemaLeft, ignoreNullable, caseSensitive)
      val right = prepareSchemaForDiff(schemaRight, ignoreNullable, caseSensitive)
      left.fields.toSet.diff(right.fields.toSet)
    }
  }

  def prepareSchemaForDiff(schemaIn: GenericSchema, ignoreNullable: Boolean, caseSensitive: Boolean, ignoreMetadata: Boolean = true): GenericSchema = {
    var schema = schemaIn
    if (ignoreNullable) schema = schema.makeNullable
    if (!caseSensitive) schema = schema.toLowerCase
    if (ignoreMetadata) schema = schema.removeMetadata
    schema
  }

  /**
   * Computes the set difference of `right` minus `left`, i.e: `Set(right)` \ `Set(left)`.
   *
   * StructField equality is defined by exact matching of the field name and partial (subset) matching of field
   * data type as computed by `deepIsTypeSubset`.
   *
   * @param ignoreNullability whether to ignore differences in nullability.
   * @return The set of fields in `right` that are not contained in `left`.
   */
  private def deepPartialMatchDiffFields(left: Seq[GenericField], right: Seq[GenericField], ignoreNullability: Boolean = false, caseSensitive: Boolean = false): Set[GenericField] = {
    val rightNamesIndex = right.groupBy(f => if (caseSensitive) f.name else f.name.toLowerCase)
    left.toSet.map { leftField: GenericField =>
      val leftName = if (caseSensitive) leftField.name else leftField.name.toLowerCase
      rightNamesIndex.get(leftName) match {
        case Some(rightFieldsWithSameName) if rightFieldsWithSameName.foldLeft(false) {
          (hasPreviousSubset, rightField) =>
            hasPreviousSubset || ( //if no previous match found check this rightField
              (ignoreNullability || leftField.nullable == rightField.nullable) //either nullability is ignored or nullability must match
                && deepIsTypeSubset(leftField.dataType, rightField.dataType, ignoreNullability, caseSensitive) //left field must be a subset of right field
              )
        } => Set.empty //found a match
        case _ => Set(leftField) //left field is not contained in right
      }
    }.flatten
  }

  /**
   * Check if a type is a subset of another type with deep comparison.
   *
   * - For simple types (e.g. String) it checks if the type names are equal.
   * - For array types it checks recursively whether the element types are subsets and optionally the containsNull property.
   * - For map types it checks recursively whether the key types and value types are subsets and optionally the valueContainsNull property.
   * - For struct types it checks whether all fields is a subset with `deepPartialMatchDiffFields`.
   *
   * @param ignoreNullability whether to ignore differences in nullability.
   * @return `true` iff `leftType` is a subset of `rightType`. `false` otherwise.
   */
  private def deepIsTypeSubset(leftType: GenericDataType, rightType: GenericDataType, ignoreNullability: Boolean, caseSensitive: Boolean ): Boolean = {
    if (leftType.typeName != rightType.typeName) false  /*fail fast*/
    else {
      (leftType, rightType) match {
        case (structL:GenericStructDataType, structR:GenericStructDataType) =>
          structL.withOtherFields(structR, (l,r) => deepPartialMatchDiffFields(l, r, ignoreNullability, caseSensitive).isEmpty)
        case (arrayL:GenericArrayDataType, arrayR:GenericArrayDataType) =>
          if (!ignoreNullability && (arrayL.containsNull != arrayR.containsNull)) false
          else arrayL.withOtherElementType(arrayR, (l,r) =>  deepIsTypeSubset(l, r, ignoreNullability, caseSensitive: Boolean))
        case (mapL:GenericMapDataType, mapR:GenericMapDataType) =>
          if (!ignoreNullability && (mapL.valueContainsNull != mapR.valueContainsNull)) false
          else mapL.withOtherKeyType(mapR, (l,r) => deepIsTypeSubset(l, r, ignoreNullability, caseSensitive)) && mapL.withOtherValueType(mapR, (l,r) => deepIsTypeSubset(l, r, ignoreNullability, caseSensitive))
        case _ => true //typeNames are equal
      }
    }
  }

  def isSparkCaseSensitive: Boolean = {
    SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
  }

}
