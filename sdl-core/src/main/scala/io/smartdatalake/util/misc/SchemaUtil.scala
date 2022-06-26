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

import com.databricks.spark.xml.util.XSDToSchema
import io.smartdatalake.config.{ConfigUtil, ConfigurationException}
import io.smartdatalake.util.hdfs.HdfsUtil
import io.smartdatalake.util.json.{SchemaConverter => JsonSchemaConverter}
import io.smartdatalake.workflow.dataframe._
import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.confluent.avro.AvroSchemaConverter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, StructType}

import scala.reflect.runtime.universe.{Type, TypeTag}

object SchemaUtil {

  /**
   * Computes the set difference between the columns of `schemaLeft` and of the columns of `schemaRight`: `Set(schemaLeft)` \ `Set(schemaRight)`.
   *
   * @param schemaLeft     schema used as minuend.
   * @param schemaRight    schema used as subtrahend.
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
   * @param ignoreNullable whether to ignore differences in nullability.
   * @return The set of fields in `right` that are not contained in `left`.
   */
  private def deepPartialMatchDiffFields(left: Seq[GenericField], right: Seq[GenericField], ignoreNullable: Boolean = false, caseSensitive: Boolean = false): Set[GenericField] = {
    val rightNamesIndex = right.groupBy(f => if (caseSensitive) f.name else f.name.toLowerCase)
    left.toSet.map { leftField: GenericField =>
      val leftName = if (caseSensitive) leftField.name else leftField.name.toLowerCase
      rightNamesIndex.get(leftName) match {
        case Some(rightFieldsWithSameName) if rightFieldsWithSameName.foldLeft(false) {
          (hasPreviousSubset, rightField) =>
            hasPreviousSubset || (//if no previous match found check this rightField
              (ignoreNullable || leftField.nullable == rightField.nullable) //either nullability is ignored or nullability must match
                && deepIsTypeSubset(leftField.dataType, rightField.dataType, ignoreNullable, caseSensitive) //left field must be a subset of right field
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

  def isSparkCaseSensitive: Boolean = {
    SQLConf.get.getConf(SQLConf.CASE_SENSITIVE)
  }

  def getSchemaFromCaseClass[T <: Product : TypeTag]: StructType = {
    Encoders.product[T].schema
  }

  def getSchemaFromCaseClass(tpe: Type): StructType = {
    ProductUtil.createEncoder(tpe).schema
  }

  def getSchemaFromJavaBean(beanClass: Class[_]): StructType = {
    JavaTypeInference.inferDataType(beanClass)._1.asInstanceOf[StructType]
  }

  def getSchemaFromJsonSchema(jsonSchemaContent: String): StructType = {
    JsonSchemaConverter.convert(jsonSchemaContent)
  }

  def getSchemaFromAvroSchema(avroSchemaContent: String): StructType = {
    AvroSchemaConverter.toSqlType(new Schema.Parser().parse(avroSchemaContent)).dataType.asInstanceOf[StructType]
  }


  def getSchemaFromXsd(xsdContent: String): StructType = {
    XSDToSchema.read(xsdContent)
  }

  def getSchemaFromDdl(ddl: String): StructType = {
    StructType.fromDDL(ddl)
  }

  /**
   * Parses a Spark [[StructType]] by using the desired schema provider.
   * The schema provider is included in the configuration value as prefix terminated by '#'.
   */
  def readSchemaFromConfigValue(schemaConfig: String): StructType = {
    import io.smartdatalake.util.misc.SchemaProviderType._
    implicit lazy val defaultHadoopConf: Configuration = new Configuration()
    val (providerId, value) = ConfigUtil.parseProviderConfigValue(schemaConfig, Some(DDL.toString))
    SchemaProviderType.withName(providerId.toLowerCase) match {
      case DDL =>
        getSchemaFromDdl(value)
      case DDLFile =>
        val content = HdfsUtil.readHadoopFile(value)
        getSchemaFromDdl(content)
      case CaseClass =>
        val clazz = this.getClass.getClassLoader.loadClass(value)
        val mirror = scala.reflect.runtime.currentMirror
        val tpe = mirror.classSymbol(clazz).toType
        getSchemaFromCaseClass(tpe)
      case JavaBean =>
        val clazz = this.getClass.getClassLoader.loadClass(value)
        getSchemaFromJavaBean(clazz)
      case XsdFile =>
        val valueElements = value.split(";")
        assert(valueElements.size == 1 || valueElements.size == 2, s"XSD schema provider configuration error. Configuration format is '<path-to-xsd-file>;<row-tag>', but received $value.")
        val path = valueElements.head
        val rowTag = valueElements.drop(1).headOption
        val content = HdfsUtil.readHadoopFile(path)
        val schema = getSchemaFromXsd(content)
        rowTag.map(t => extractRowTag(schema, t)).getOrElse(schema)
      case JsonSchemaFile =>
        val valueElements = value.split(";")
        assert(valueElements.size == 1 || valueElements.size == 2, s"Json schema provider configuration error. Configuration format is '<path-to-json-file>;<row-tag>', but received $value.")
        val path = valueElements.head
        val rowTag = valueElements.drop(1).headOption
        val content = HdfsUtil.readHadoopFile(path)
        val schema = getSchemaFromJsonSchema(content)
        rowTag.map(t => extractRowTag(schema, t)).getOrElse(schema)
      case AvroSchemaFile =>
        val valueElements = value.split(";")
        assert(valueElements.size == 1 || valueElements.size == 2, s"Avro schema provider configuration error. Configuration format is '<path-to-avsc-file>;<row-tag>', but received $value.")
        val path = valueElements.head
        val rowTag = valueElements.drop(1).headOption
        val content = HdfsUtil.readHadoopFile(path)
        val schema = getSchemaFromAvroSchema(content)
        rowTag.map(t => extractRowTag(schema, t)).getOrElse(schema)
    }
  }

  /**
   * extract nested schema element according to row tag.
   */
  private[smartdatalake] def extractRowTag(schema:StructType, rowTag: String) = {
    rowTag.split("/").filter(_.nonEmpty).foldLeft(schema){
      case (schema, element) =>
        val schemaElement = schema.fields.find(_.name == element)
        assert(schemaElement.isDefined, s"Schema element $element not found while extracting rowTag. Available fields are ${schema.fieldNames.mkString(", ")}")
        var elementDataType = schemaElement.get.dataType
        if (elementDataType.isInstanceOf[ArrayType]) elementDataType = elementDataType.asInstanceOf[ArrayType].elementType
        assert(elementDataType.isInstanceOf[StructType], s"Schema element $element dataType is ${elementDataType.typeName}, but must be a StructType.")
        elementDataType.asInstanceOf[StructType]
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
   * Get schema from an XSD file (XML schema definition), using spark-xml's XSD support: [[https://github.com/databricks/spark-xml#xsd-support]]
   * Parameters (semicolon separated):
   * - the hadoop path of the XSD file.
   * - row tag to extract a subpart from the schema, see also XML source rowTag option. Put an emtpy string to use root tag.
   *   To extract a nested row tag, split the elements by slash (/).
   */
  val XsdFile: SchemaProviderType.Value = Value("xsdfile")

  /**
   * Get schema from an Json Schema file, using an adapted verion of zalando-incubator/spark-json-schema library, see also [[JsonSchemaConverter]]
   * Parameters (semicolon separated):
   * - the hadoop path of the Json schema file.
   * - row tag to extract a subpart from the schema, this is similar to XML source rowTag option. Put an emtpy string to use root tag.
   *   To extract a nested row tag, split the elements by slash (/).
   */
  val JsonSchemaFile: SchemaProviderType.Value = Value("jsonschemafile")

  /**
   * Get schema from an Avro Schema file using methods from spark-avro
   * Parameters (semicolon separated):
   * - the hadoop path of the Avro schema file.
   * - row tag to extract a subpart from the schema, this is similar to XML source rowTag option. Put an emtpy string to use root tag.
   *   To extract a nested row tag, split the elements by slash (/).
   */
  val AvroSchemaFile: SchemaProviderType.Value = Value("avroschemafile")
}