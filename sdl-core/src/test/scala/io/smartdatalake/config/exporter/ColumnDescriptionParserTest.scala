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
package io.smartdatalake.config.exporter

import org.apache.hadoop.conf.Configuration
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JNothing, JString, JValue}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class ColumnDescriptionParserTest extends AnyFunSuite {

  test("parse column descriptions of a markdown file") {
    val content =
      """# Some DataObject
        |
        |Some free text which is not a column description.
        |
        |## Columns
        |@column a  Description of a
        |@column "b" Description of b,
        |continued on the next line
        |
        |@column `c.c1` Description of a nested column
        |
        |# Next header closes the last description
        |This text is ignored.
        |""".stripMargin
    val descriptions = ColumnDescriptionParser.parseContent(content)
    assert(descriptions("a") == "Description of a")
    assert(descriptions("b") == s"Description of b,${System.lineSeparator()}continued on the next line")
    assert(descriptions("c.c1") == "Description of a nested column")
    assert(descriptions.size == 3)
  }

  test("column names are converted to column paths, dropping array markers") {
    assert(ColumnDescriptionParser.toColumnPath("a") == Seq("a"))
    assert(ColumnDescriptionParser.toColumnPath("c.c1") == Seq("c", "c1"))
    assert(ColumnDescriptionParser.toColumnPath("b.[].b1") == Seq("b", "b1"))
  }

  test("a missing description directory results in no descriptions") {
    implicit val hadoopConf: Configuration = new Configuration()
    val descriptionPath = Files.createTempDirectory("descriptions").resolve("notExisting")
    assert(ColumnDescriptionParser.parse(descriptionPath.toUri.toString).isEmpty)
  }

  private val schemaJson = JsonMethods.parse(
    """[
      |  {"name": "a", "dataType": "string", "nullable": true, "comment": "schema comment of a"},
      |  {"name": "Upper", "dataType": "string", "nullable": true},
      |  {"name": "s", "dataType": {"dataType": "struct", "fields": [
      |    {"name": "s1", "dataType": "string", "nullable": true}
      |  ]}, "nullable": true},
      |  {"name": "b", "dataType": {"dataType": "array", "elementType": {"dataType": "struct", "fields": [
      |    {"name": "b1", "dataType": "string", "nullable": true},
      |    {"name": "b2", "dataType": "string", "nullable": true}
      |  ]}}, "nullable": true},
      |  {"name": "m", "dataType": {"dataType": "map", "keyType": "string", "valueType": {"dataType": "struct", "fields": [
      |    {"name": "x", "dataType": "string", "nullable": true}
      |  ]}}, "nullable": true}
      |]""".stripMargin).asInstanceOf[JArray]

  private def field(json: JValue, name: String): JValue = json.find(f => (f \ "name") == JString(name)).get
  private def fieldOfArray(json: JArray, name: String): JValue = json.arr.find(f => (f \ "name") == JString(name)).get

  test("merge descriptions into top level and nested columns of the schema json") {
    val descriptions = Map(
      "a" -> "description of a",
      "upper" -> "description of Upper",
      "s.s1" -> "description of s.s1",
      "b.[].b1" -> "description of b.b1",
      "b.b2" -> "description of b.b2, array traversed transparently",
      "m.value.x" -> "description of m.value.x"
    )
    val (merged, unresolved) = ColumnDescriptionParser.mergeIntoSchemaJson(schemaJson, descriptions)
    assert(unresolved.isEmpty)
    // description overrides the comment of the schema
    assert(fieldOfArray(merged, "a") \ "comment" == JString("description of a"))
    // column names are compared case-insensitive
    assert(fieldOfArray(merged, "Upper") \ "comment" == JString("description of Upper"))
    assert(field(fieldOfArray(merged, "s") \ "dataType" \ "fields", "s1") \ "comment" == JString("description of s.s1"))
    val bFields = fieldOfArray(merged, "b") \ "dataType" \ "elementType" \ "fields"
    assert(field(bFields, "b1") \ "comment" == JString("description of b.b1"))
    assert(field(bFields, "b2") \ "comment" == JString("description of b.b2, array traversed transparently"))
    assert(field(fieldOfArray(merged, "m") \ "dataType" \ "valueType" \ "fields", "x") \ "comment" == JString("description of m.value.x"))
    // the other attributes are kept
    assert(fieldOfArray(merged, "a") \ "dataType" == JString("string"))
    assert(fieldOfArray(merged, "b") \ "comment" == JNothing)
  }

  test("merge descriptions of array elements and map keys and values as attributes of the data type") {
    val descriptions = Map(
      "b.[]" -> "description of the elements of b",
      "m.key" -> "description of the keys of m",
      "m.value" -> "description of the values of m"
    )
    val (merged, unresolved) = ColumnDescriptionParser.mergeIntoSchemaJson(schemaJson, descriptions)
    assert(unresolved.isEmpty)
    assert(fieldOfArray(merged, "b") \ "comment" == JNothing)
    assert(fieldOfArray(merged, "b") \ "dataType" \ "elementComment" == JString("description of the elements of b"))
    assert(fieldOfArray(merged, "m") \ "dataType" \ "keyComment" == JString("description of the keys of m"))
    assert(fieldOfArray(merged, "m") \ "dataType" \ "valueComment" == JString("description of the values of m"))
  }

  test("descriptions of columns not found in the schema json are returned as unresolved") {
    val descriptions = Map("a" -> "description of a", "notExisting" -> "?", "s.notExisting" -> "?", "a.[]" -> "?", "s.key" -> "?")
    val (merged, unresolved) = ColumnDescriptionParser.mergeIntoSchemaJson(schemaJson, descriptions)
    assert(unresolved.toSet == Set("notExisting", "s.notExisting", "a.[]", "s.key"))
    assert(fieldOfArray(merged, "a") \ "comment" == JString("description of a"))
  }
}
