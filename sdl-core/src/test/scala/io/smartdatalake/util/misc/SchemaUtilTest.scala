/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.dataframe.{GenericArrayDataType, GenericStructDataType}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{a, be}

import java.nio.file.Files

class SchemaUtilTest extends FunSuite {

  private val tempDir = Files.createTempDirectory("schema-util-test")

  // copy xsd file from resource to filesystem
  private val xsdResourceFile = "xmlSchema/basket.xsd"
  private val xsdFile = tempDir.resolve(xsdResourceFile).toFile
  TestUtil.copyResourceToFile(xsdResourceFile, xsdFile)

  // copy json file from resource to filesystem
  private val jsonSchemaResourceFile = "jsonSchema/testJsonSchema.json"
  private val jsonSchemaFile = tempDir.resolve(jsonSchemaResourceFile).toFile
  TestUtil.copyResourceToFile(jsonSchemaResourceFile, jsonSchemaFile)

  // copy avsc file from resource to filesystem
  private val avroSchemaResourceFile = "avscSchema/testAvroSchema.avsc"
  private val avroSchemaFile = tempDir.resolve(avroSchemaResourceFile).toFile
  TestUtil.copyResourceToFile(avroSchemaResourceFile, avroSchemaFile)

  // copy ddl file from resource to filesystem
  private val ddlSchemaResourceFile = "ddlSchema/testDDLSchema.ddl"
  private val ddlSchemaFile = tempDir.resolve(ddlSchemaResourceFile).toFile
  TestUtil.copyResourceToFile(ddlSchemaResourceFile, ddlSchemaFile)

  test("parse ddl schema") {
    val schemaConfig = s"${SchemaProviderType.DDL.toString}#a int, b string"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("a", "b"))
  }

  test("parse ddl schema is default schema provider") {
    val schemaConfig = s"a int, b string"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("a", "b"))
  }

  test("parse ddl schema from file") {
    val schemaConfig = s"${SchemaProviderType.DDLFile.toString}#${ddlSchemaFile.toString}"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("a", "b"))
  }

  test("parse ddl schema from file and throws error") {
    val schemaConfig = s"${SchemaProviderType.DDLFile.toString}#${ddlSchemaFile.toString};a"
    a [AssertionError] should be thrownBy  SchemaUtil.readSchemaFromConfigValue(schemaConfig)
  }

  test("parse ddl schema from file as a file from classpath") {
    val schemaConfig = s"${SchemaProviderType.DDLFile.toString}#cp:/${ddlSchemaResourceFile.toString}"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("a", "b"))
  }

  test("parse schema from case class, enrich with comments") {
    val schemaConfig = s"${SchemaProviderType.CaseClass.toString}#${classOf[TestSchema].getName}"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("a", "b", "c"))
    assert(schema.fields.find(_.name == "a").flatMap(_.comment).contains("TestA"))
    assert(schema.fields.find(_.name == "b").flatMap(_.comment).contains("TestB"))
    val structB = schema.fields.find(_.name == "b").map(_.dataType).collect { case x: GenericStructDataType => x }.get
    assert(structB.fields.find(_.name == "x").flatMap(_.comment).contains("TestX"))
    val elementStructC = schema.fields.find(_.name == "c").map(_.dataType).collect { case x: GenericArrayDataType => x }.get.elementDataType.asInstanceOf[GenericStructDataType]
    assert(elementStructC.fields.find(_.name == "y").flatMap(_.comment).contains("TestY"))
  }

  test("parse xsd schema with row tag") {
    val schemaConfig = s"${SchemaProviderType.XsdFile.toString}#${xsdFile.toString};basket"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("entry"))
  }

  test("parse xsd schema with row tag as a file from classpath") {
    val schemaConfig = s"${SchemaProviderType.XsdFile.toString}#cp:/${xsdResourceFile};basket"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("entry"))
  }

  test("parse xsd schema with row tag and jsonCompatibility") {
    val schemaConfig = s"${SchemaProviderType.XsdFile.toString}#${xsdFile.toString};basket;10;true"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("entrys"))
  }

  test("parse xsd schema with nested row tag and extract array type") {
    val schemaConfig = s"${SchemaProviderType.XsdFile.toString}#${xsdFile.toString};basket/entry"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("key", "value"))
  }

  test("parse json schema with nested row tag") {
    val schemaConfig = s"${SchemaProviderType.JsonSchemaFile.toString}#${jsonSchemaFile.toString};structure/nestedArray"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("key", "value"))
  }

  test("parse json schema with nested row tag as a file from classpath") {
    val schemaConfig = s"${SchemaProviderType.JsonSchemaFile.toString}#cp:/${jsonSchemaResourceFile};structure/nestedArray"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("key", "value"))
  }

  test("parse avro schema") {
    val schemaConfig = s"${SchemaProviderType.AvroSchemaFile.toString}#${avroSchemaFile.toString};"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("id", "username", "passwordHash", "signupDate", "emailAddresses"))
  }

  test("parse avro schema as a file from classpath") {
    val schemaConfig = s"${SchemaProviderType.AvroSchemaFile.toString}#cp:/${avroSchemaResourceFile};"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.columns == Seq("id", "username", "passwordHash", "signupDate", "emailAddresses"))
  }

}

/**
 * This is a test schema.
 *
 * @param a TestA
 * @param b TestB
 */
case class TestSchema(a: Int, b: TestSubType, c: Seq[TestSubType])

/**
 * This is a test schema.
 *
 * @param x TestX
 * @param y TestY
 */
case class TestSubType(x: String, y: String)
