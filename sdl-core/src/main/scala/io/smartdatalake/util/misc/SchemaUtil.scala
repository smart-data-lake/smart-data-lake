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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

object SchemaUtil {

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight`: `Set(schemaLeft)` \ `Set(schemaRight)`.
   *
   * @param schemaLeft schema used as minuend.
   * @param schemaRight schema used as subtrahend.
   * @param ignoreNullable if `true`, columns that only differ in their `nullable` property are considered equal.
   * @return the set of columns contained in `schemaRight` but not in `schemaLeft`.
   */
  def schemaDiff(schemaLeft: StructType, schemaRight: StructType, ignoreNullable: Boolean = false, caseSensitive: Boolean = false, deep: Boolean = false): Set[StructField] = {
    if (deep) {
      deepPartialMatchDiffFields(schemaLeft.fields, schemaRight.fields, ignoreNullable, caseSensitive)
    } else {
      val left = prepareSchemaForDiff(schemaLeft, ignoreNullable, caseSensitive).toSet
      val right = prepareSchemaForDiff(schemaRight, ignoreNullable, caseSensitive).toSet
      left.diff(right)
    }
  }

  def prepareSchemaForDiff(schemaIn: StructType, ignoreNullable: Boolean, caseSensitive: Boolean, ignoreMetadata: Boolean = true): Seq[StructField] = {
    var schema = schemaIn.fields.toSeq
    if (ignoreNullable) schema = nullableFields(schema)
    if (!caseSensitive) schema = lowerCaseFields(schema)
    if (ignoreMetadata) schema = removeMetadataFields(schema)
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
  private def deepPartialMatchDiffFields(left: Array[StructField], right: Array[StructField], ignoreNullability: Boolean = false, caseSensitive: Boolean = false): Set[StructField] = {

    val rightNamesIndex = if (caseSensitive) right.groupBy(_.name) else right.groupBy(_.name.toLowerCase)
    left.toSet.flatMap[StructField, Set[StructField]] { leftField =>
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
    }
  }

  /**
   * Check if a type is a subset of another type with deep comparison.
   *
   * - For simple types (e.g., [[StringType]]) it checks if the type names are equal.
   * - For [[ArrayType]] it checks recursively whether the element types are subsets and optionally the containsNull property.
   * - For [[MapType]] it checks recursively whether the key types and value types are subsets and optionally the valueContainsNull property.
   * - For [[StructType]] it checks whether all fields is a subset with `deepPartialMatchDiffFields`.
   *
   * @param ignoreNullability whether to ignore differences in nullability.
   * @return `true` iff `leftType` is a subset of `rightType`. `false` otherwise.
   */
  private def deepIsTypeSubset(leftType: DataType, rightType: DataType, ignoreNullability: Boolean, caseSensitive: Boolean ): Boolean = {
    if (leftType.typeName != rightType.typeName) false  /*fail fast*/ else {
      (leftType, rightType) match {
        case (StructType(fieldsL), StructType(fieldsR)) => deepPartialMatchDiffFields(fieldsL, fieldsR, ignoreNullability, caseSensitive).isEmpty
        case (ArrayType(elementTpeL, containsNullL), ArrayType(elementTpeR, containsNullR)) =>
          if (!ignoreNullability && (containsNullL != containsNullR)) false else {
            deepIsTypeSubset(elementTpeL, elementTpeR, ignoreNullability, caseSensitive: Boolean)
          }
        case (MapType(keyTpeL, valTpeL, valContainsNullL), MapType(keyTpeR, valTpeR, valContainsNullR)) =>
          if (!ignoreNullability && (valContainsNullL != valContainsNullR)) false else {
            deepIsTypeSubset(keyTpeL, keyTpeR, ignoreNullability, caseSensitive) && deepIsTypeSubset(valTpeL, valTpeR, ignoreNullability, caseSensitive)
          }
        case _ => true //names are equal
      }
    }
  }

  private def nullableFields(fields: Seq[StructField]): Seq[StructField] = {
    fields.map(field => field.copy(
      dataType = nullableDataType(field.dataType),
      nullable = true
    ))
  }

  private def removeMetadataFields(fields: Seq[StructField]): Seq[StructField] = {
    fields.map(field => field.copy(
      dataType = removeMetadataDataType(field.dataType),
      metadata = Metadata.empty
    ))
  }
  private def lowerCaseFields(fields: Seq[StructField]): Seq[StructField] = {
    fields.map(field => field.copy(name = field.name.toLowerCase))
  }

  private def nullableDataType(dataType: DataType): DataType = {
    dataType match {
      case struct: StructType => StructType(
        fields = nullableFields(struct)
      )
      case ArrayType(elementType, _) => ArrayType(
        nullableDataType(elementType),
        containsNull = true
      )
      case MapType(keyType, valueType, _) => MapType(
        nullableDataType(keyType),
        nullableDataType(valueType),
        valueContainsNull = true
      )
      case _ => dataType
    }
  }

  private def removeMetadataDataType(dataType: DataType): DataType = {
    dataType match {
      case struct: StructType => StructType(
        fields = removeMetadataFields(struct)
      )
      case ArrayType(elementType, containsNull) => ArrayType(
        removeMetadataDataType(elementType),
        containsNull = containsNull
      )
      case MapType(keyType, valueType, valueContainsNull) => MapType(
        removeMetadataDataType(keyType),
        removeMetadataDataType(valueType),
        valueContainsNull = valueContainsNull
      )
      case _ => dataType
    }
  }

  def isSparkCaseSensitive: Boolean = {
    SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
  }

}
