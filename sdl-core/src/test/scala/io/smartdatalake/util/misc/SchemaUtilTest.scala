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
import org.scalatest.FunSuite

import java.nio.file.Files

class SchemaUtilTest extends FunSuite {

  private val tempDir = Files.createTempDirectory("schema-util-test")

  // copy xsd file from resource to filesystem
  private val xsdResourceFile = "xmlSchema/basket.xsd"
  private val xsdFile = tempDir.resolve(xsdResourceFile).toFile
  TestUtil.copyResourceToFile(xsdResourceFile, xsdFile)

  // copy xsd file from resource to filesystem
  private val jsonSchemaResourceFile = "jsonSchema/testJsonSchema.json"
  private val jsonSchemaFile = tempDir.resolve(jsonSchemaResourceFile).toFile
  TestUtil.copyResourceToFile(jsonSchemaResourceFile, jsonSchemaFile)

  test("parse ddl schema") {
    val schemaConfig = s"${SchemaProviderType.DDL.toString}#a int, b string"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.fieldNames.toSeq == Seq("a", "b"))
  }

  test("parse ddl schema is default schema provider") {
    val schemaConfig = s"a int, b string"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.fieldNames.toSeq == Seq("a", "b"))
  }

  test("parse schema from case class") {
    val schemaConfig = s"${SchemaProviderType.CaseClass.toString}#${classOf[TestSchema].getName}"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.fieldNames.toSeq == Seq("a", "b"))
  }

  test("parse xsd schema with row tag") {
    val schemaConfig = s"${SchemaProviderType.XsdFile.toString}#${xsdFile.toString};basket"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.fieldNames.toSeq == Seq("entry"))
  }

  test("parse xsd schema with nested row tag and extract array type") {
    val schemaConfig = s"${SchemaProviderType.XsdFile.toString}#${xsdFile.toString};basket/entry"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.fieldNames.toSeq == Seq("key", "value"))
  }

  test("parse json schema with nested row tag") {
    val schemaConfig = s"${SchemaProviderType.JsonSchemaFile.toString}#${jsonSchemaFile.toString};structure/nestedArray"
    val schema = SchemaUtil.readSchemaFromConfigValue(schemaConfig)
    assert(schema.fieldNames.toSeq == Seq("key", "value"))
  }

}

case class TestSchema(a: Int, b: String)
