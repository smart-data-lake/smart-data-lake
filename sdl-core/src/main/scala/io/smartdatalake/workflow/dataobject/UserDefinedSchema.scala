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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.dataframe.GenericSchema

/**
 * A [[DataObject]] that allows optional user-defined schema definition.
 *
 * Some [[DataObject]]s support optional schema inference.
 * Specifying the schema attribute disables automatic schema inference.

 * Note: This is only used by the functionality defined in [[CanCreateDataFrame]], that is,
 * when reading data frames from the underlying data container.
 * [[io.smartdatalake.workflow.action.Action]]s that bypass data frames ignore the `schema` attribute
 * if it is defined.
 */
trait UserDefinedSchema {

  /**
   * An optional user-defined schema definition for this DataObject.
   * If defined, any automatic schema inference is avoided.
   *
   * The schema corresponds to the schema on write, it must not include optional columns on read, e.g. the filenameColumn for SparkFileDataObjects.
   *
   * Define the schema by using one of the schema providers below, default is DDL.
   * The schema provider and its configuration value must be provided in the format <PROVIDERID>#<VALUE>.
   *
   * Schema Providers available are (see also [[io.smartdatalake.util.misc.SchemaProviderType]]):
   * - ddl: create the schema from a Spark ddl string, e.g. `ddl#a string, b array<struct<b1: string, b2: long>>, c struct<c1: string, c2: long>`
   * - ddlFile: read a Spark ddl definition from a file and create a schema, e.g. `ddlFile#abc/xyz.ddl`
   * - caseClass: convert a Scala Case Class to a schema using Spark encoders, e.g. `caseClass#com.sample.XyzClass`
   * - javaBean: convert a Java Bean to a schema using Spark encoders, e.g. `javaBean#com.sample.XyzClass`
   * - xsdFile: read an Xml Schema Definition file and create a schema, e.g. `xsdFile#abc/xyz.xsd`
   *   The following parameters allow to customize the behavior: `xsdFile#<path-to-xsd-file>;<row-tag>;<maxRecursion:Int>;<jsonCompatibility:Boolean>`
   *   <row-tag>: configure the path of the element to extract from the xsd schema. Leave empty to extract the root.
   *   <maxRecursion>: if xsd schema is recursive, this configures the number of levels to create in the schema.
   *   Default is 10 levels.
   *   <jsonCompatibility>: In XML array elements are modeled with their own tag named with singular name.
   *   In JSON an array attribute has unnamed array entries, but the array attribute has a plural name.
   *   If true, the singular name of the array element in the XSD is converted to a plural name by adding an 's'
   *   in order to read corresponding json files.
   *   Default is false.
   * - jsonSchemaFile: read a Json Schema file and create a schema, e.g. `jsonSchemaFile#abc/xyz.json`
   *   The following parameters allow to customize the behavior: `jsonSchemaFile#<path-to-json-file>;<row-tag>;<strictTyping:Boolean>;<additionalPropertiesDefault:Boolean>`
   *   <row-tag>: configure the path of the element to extract from the json schema. Leave empty to extract the root.
   *   <strictTyping>: if true
   *   union types (oneOf) are merged if rational, otherwise they are simply mapped to StringType;
   *   additional properties are ignored, otherwise the corresponding schema object is mapped to MapType(String,String).
   *   Default is strictTyping=false.
   *   <additionalPropertiesDefault>: Set to true or false.
   *   This is used as default value for 'additionalProperties'-field if it is missing in a schema with type='object'.
   *   Default value is additionalPropertiesDefault=true, as this is conform with the specification.
   * - avroSchemaFile: read an Avro Schema file and create a schema, e.g. `avroSchemaFile#abc/xyz.avsc`
   *   The following parameters allow to customize the behavior: `avroSchemaFile#<path-to-avsc-file>;<row-tag>`
   *   <row-tag>: configure the path of the element to extract from the avro schema. Leave empty to extract the root.
   *
   * Note that all schema files are configured as Hadoop path. The custom prefix 'cp' can be used to read schema files
   * from the classpath, e.g. `xsdFile#cp:/xyz.xsd`.
   */
  def schema: Option[GenericSchema]
}