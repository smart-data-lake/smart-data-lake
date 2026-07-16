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
package io.smartdatalake.util.misc

import io.smartdatalake.workflow.dataframe._

import scala.annotation.tailrec

// TODO: Merge with package io.smartdatalake.util.spark.dataset
object SchemaUtil {

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight`: `Set(schemaLeft)` \ `Set(schemaRight)`.
   *
   * @param schemaLeft     schema used as minuend.
   * @param schemaRight    schema used as subtrahend.
   * @param ignoreNullable if `true`, columns that only differ in their `nullable` property are considered equal.
   * @return the set of columns contained in `schemaRight` but not in `schemaLeft`.
   */
  def schemaDiff(schemaLeft: GenericSchema, schemaRight: GenericSchema,
                 ignoreNullable: Boolean = false, caseSensitive: Boolean = false, deep: Boolean = false): Set[GenericField] = {
    if (deep) {
      deepPartialMatchDiffFields(schemaLeft.fields, schemaRight.fields, ignoreNullable, caseSensitive)
    } else {
      val left = prepareSchemaForDiff(schemaLeft.fields, ignoreNullable, caseSensitive)
      val right = prepareSchemaForDiff(schemaRight.fields, ignoreNullable, caseSensitive)
      left.toSet.diff(right.toSet)
    }
  }

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight` in both directions:
   * 1st return value is `Set(schemaLeft) \ Set(schemaRight)`, 2nd return value is `Set(schemaRight) \ Set(schemaLeft)`.
   *
   * @return Tuple `Set(schemaLeft) \ Set(schemaRight), `Set(schemaRight) \ Set(schemaLeft)`
   */
  def schemaDiff2(schemaLeft: Seq[GenericField], schemaRight: Seq[GenericField], ignoreNullable: Boolean = false, caseSensitive: Boolean = false, deep: Boolean = false): (Set[GenericField], Set[GenericField]) = {
    if (deep) {
      (
        deepPartialMatchDiffFields(schemaLeft, schemaRight, ignoreNullable, caseSensitive),
        deepPartialMatchDiffFields(schemaRight, schemaLeft, ignoreNullable, caseSensitive),
      )
    } else {
      val left = prepareSchemaForDiff(schemaLeft, ignoreNullable, caseSensitive).toSet
      val right = prepareSchemaForDiff(schemaRight, ignoreNullable, caseSensitive).toSet
      (left.diff(right), right.diff(left))
    }
  }

  def prepareSchemaForDiff(schemaIn: Seq[GenericField],
                           ignoreNullable: Boolean, caseSensitive: Boolean, ignoreMetadata: Boolean = true): Seq[GenericField] = {
    var schema = schemaIn
    if (ignoreNullable) schema = schema.map(_.makeNullable)
    if (!caseSensitive) schema = schema.map(_.toLowerCase)
    if (ignoreMetadata) schema = schema.map(_.removeMetadata)
    schema
  }

  /**
   * Computes the set difference of `left` minus `right`, i.e: `Set(left)` \ `Set(right)`.
   *
   * StructField equality is defined by exact matching of the field name and partial (subset) matching of field
   * data type as computed by `deepIsTypeSubset`.
   *
   * @param ignoreNullable whether to ignore differences in nullability.
   * @return The set of fields in `left` that are not contained in `right`.
   *
   *         TODO #935: probably does not work for structs nested in arrays...
   */
  private[smartdatalake] def deepPartialMatchDiffFields(left: Seq[GenericField],
                                                        right: Seq[GenericField],
                                                        ignoreNullable: Boolean = false,
                                                        caseSensitive: Boolean = false): Set[GenericField] = {
    val rightNamesIndex = right.groupBy(f => if (caseSensitive) f.name else f.name.toLowerCase)
    left.map { leftField: GenericField =>
      val leftName = if (caseSensitive) leftField.name else leftField.name.toLowerCase
      rightNamesIndex.get(leftName) match {
        case Some(rightFieldsWithSameName) if rightFieldsWithSameName.foldLeft(false) {
          (hasPreviousSubset, rightField) =>
            hasPreviousSubset || (//if no previous match found check this rightField
              (ignoreNullable || leftField.nullable == rightField.nullable) //either nullability is ignored or nullability must match
                && deepIsTypeSubset(leftField.dataType, rightField.dataType, ignoreNullable, caseSensitive) //left field must be a subset of right field
              )
        } => Set.empty[GenericField] //found a match
        case _ => Set(leftField) //left field is not contained in right
      }
    }.toSet.flatten
  }

  /**
   * Check if a type is a subset of another type with deep comparison.
   *
   * - For simple types (e.g. String) it checks if the type names are equal.
   * - For array types it checks recursively whether the element types are subsets and optionally the containsNull property.
   * - For map types it checks recursively whether the key types and value types are subsets and optionally the valueContainsNull property.
   * - For struct types it checks whether all fields are a subset with `deepPartialMatchDiffFields`.
   *
   * @param ignoreNullable whether to ignore differences in nullability.
   * @return `true` iff `leftType` is a subset of `rightType`. `false` otherwise.
   */
  private def deepIsTypeSubset(leftType: GenericDataType, rightType: GenericDataType, ignoreNullable: Boolean, caseSensitive: Boolean): Boolean = {
    if (leftType.typeName != rightType.typeName) false /*fail fast*/
    else {
      (leftType, rightType) match {
        case (structL: GenericStructDataType, structR: GenericStructDataType) =>
          structL.withOtherFields(structR, (l, r) => deepPartialMatchDiffFields(l, r, ignoreNullable, caseSensitive).isEmpty)
        case (arrayL: GenericArrayDataType, arrayR: GenericArrayDataType) =>
          if (!ignoreNullable && (arrayL.containsNull != arrayR.containsNull)) false
          else arrayL.withOtherElementType(arrayR, (l, r) => deepIsTypeSubset(l, r, ignoreNullable, caseSensitive: Boolean))
        case (mapL: GenericMapDataType, mapR: GenericMapDataType) =>
          if (!ignoreNullable && (mapL.valueContainsNull != mapR.valueContainsNull)) false
          else mapL.withOtherKeyType(mapR, (l, r) => deepIsTypeSubset(l, r, ignoreNullable, caseSensitive)) && mapL.withOtherValueType(mapR, (l, r) => deepIsTypeSubset(l, r, ignoreNullable, caseSensitive))
        case _ => true //typeNames are equal
      }
    }
  }

  def checkMissingCols(colsLeft: Seq[String], colsRight: Seq[String], caseSensitive: Boolean): Seq[String] = {
    if (caseSensitive) colsLeft.diff(colsRight)
    else colsLeft.map(_.toLowerCase).diff(colsRight.map(_.toLowerCase))
  }

  def checkPartitionMatch(configuredPartitions: Seq[String], existingPartitions: Seq[String], caseSensitive: Boolean): (Boolean, Set[String], Set[String]) = {
    val (confPartitions, existPartitions) = if (caseSensitive) (configuredPartitions.toSet, existingPartitions.toSet)
    else (configuredPartitions.map(_.toLowerCase()).toSet, existingPartitions.map(_.toLowerCase()).toSet)
    (confPartitions == existPartitions, confPartitions, existPartitions)
  }

  /**
   * Provider for Spark-specific schema reading. Set by sdl-spark at startup.
   * If not set, schema reading always returns a LazyGenericSchema.
   */
  private[smartdatalake] var _schemaReaderProvider: Option[(String, Boolean) => GenericSchema] = None

  private[smartdatalake] def registerSchemaProvider(provider: (String, Boolean) => GenericSchema): Unit = {
    _schemaReaderProvider = Some(provider)
  }

  /**
   * Parses a schema from a config value string by delegating to the registered schema provider.
   * The schema provider is included in the configuration value as prefix terminated by '#'.
   * When no provider is registered (e.g., when running without Spark), returns a LazyGenericSchema.
   */
  def readSchemaFromConfigValue(schemaConfig: String, lazyFileReading: Boolean = true): GenericSchema = {
    _schemaReaderProvider match {
      case Some(provider) => provider(schemaConfig, lazyFileReading)
      case None => LazyGenericSchema(schemaConfig)
    }
  }
}

object SchemaProviderType extends Enumeration {
  type SchemaProviderType = Value

  /**
   * Parse SQL DDL (data definition language) using Spark.
   * Parameter: A DDL-formatted string. This is a comma separated list of field definitions, e.g. 'a INT, b STRING'.
   */
  val DDL: SchemaProviderType.Value = Value("ddl")

  /**
   * Parse SQL DDL (data definition language) using Spark from a file.
   * Parameter: the hadoop path of the file with a DDL-formatted string as content, see also DDL.
   */
  val DDLFile: SchemaProviderType.Value = Value("ddlfile")

  /**
   * Get schema from a case class using Spark Encoders.
   * Parameter: the class name of the case class.
   */
  val CaseClass: SchemaProviderType.Value = Value("caseclass")

  /**
   * Get schema from a java bean using Sparks java type inference.
   * Parameter: the class name of the java bean.
   */
  val JavaBean: SchemaProviderType.Value = Value("javabean")

  /**
   * Get schema from an XSD file (XML schema definition).
   * This is using a customized version of spark-xml's XSD support:
   * [[https://github.com/databricks/spark-xml#xsd-support]]
   * Parameters (semicolon separated):
   * - the hadoop path of the XSD file.
   * - row tag to extract a subpart from the schema, see also XML source rowTag option.
   * Put an empty string to use root tag.
   * To extract a nested row tag, split the elements by slash (/).
   */
  val XsdFile: SchemaProviderType.Value = Value("xsdfile")

  /**
   * Get schema from a JSON Schema file, using an adapted version of zalando-incubator/spark-json-schema library,
   * see also [[JsonSchemaConverter]]
   * Parameters (semicolon separated):
   * - the hadoop path of the JSON schema file.
   * - row tag to extract a subpart from the schema, this is similar to XML source rowTag option.
   * Put an empty string to use root tag.
   * To extract a nested row tag, split the elements by slash (/).
   */
  val JsonSchemaFile: SchemaProviderType.Value = Value("jsonschemafile")

  /**
   * Get schema from an Avro Schema file using methods from spark-avro
   * Parameters (semicolon separated):
   * - the hadoop path of the Avro schema file.
   * - row tag to extract a subpart from the schema, this is similar to XML source rowTag option.
   * Put an empty string to use root tag.
   * To extract a nested row tag, split the elements by slash (/).
   */
  val AvroSchemaFile: SchemaProviderType.Value = Value("avroschemafile")

  /**
   * Get schema from OpenApi specification operation
   * Parameters (semicolon separated):
   * - baseUrl
   * - operationId
   * - optional apiDocsPath, default is v3/api-docs
   * - optional responseContentType, default is application/json
   */
  val OpenApi: SchemaProviderType.Value = Value("openapi")

}