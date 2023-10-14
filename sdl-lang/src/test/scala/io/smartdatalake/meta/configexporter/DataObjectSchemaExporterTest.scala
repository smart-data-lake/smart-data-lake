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

package io.smartdatalake.meta.configexporter

import org.json4s.StringInput
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite

import java.io.File

class DataObjectSchemaExporterTest extends FunSuite {

  test("export simple schema") {
    val exporterConfig = DataObjectSchemaExporterConfig(Seq(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath), includeRegex = "dataObjectCsv1")
    val actualOutput = DataObjectSchemaExporter.exportSchemas(exporterConfig).head
    assert(actualOutput._1.id == "dataObjectCsv1")

    val expectedFieldsJson = """
      |  [ {
      |    "name" : "a",
      |    "dataType" : "string",
      |    "nullable" : true
      |  }, {
      |    "name" : "b",
      |    "dataType" : "string",
      |    "nullable" : true
      |  }, {
      |    "name" : "c",
      |    "dataType" : "string",
      |    "nullable" : true
      |  } ]
      |""".stripMargin
    val expectedFields = JsonMethods.parse(StringInput(expectedFieldsJson)).values
    val actualFields = JsonMethods.parse(StringInput(actualOutput._2)).values
    assert(actualFields == expectedFields)
  }

  test("export complex schema") {
    val exporterConfig = DataObjectSchemaExporterConfig(Seq(getClass.getResource("/dagexporter/dagexporterTest.conf").getPath), includeRegex = "dataObjectParquet6")
    val actualOutput = DataObjectSchemaExporter.exportSchemas(exporterConfig).head
    assert(actualOutput._1.id == "dataObjectParquet6")
    val expectedFieldsJson =
      """
        |  [ {
        |    "name" : "a",
        |    "dataType" : "string",
        |    "nullable" : true
        |  }, {
        |    "name" : "b",
        |    "dataType" : {
        |      "dataType" : "array",
        |      "elementType" : {
        |        "dataType" : "struct",
        |        "fields" : [ {
        |          "name" : "b1",
        |          "dataType" : "string",
        |          "nullable" : true
        |        }, {
        |          "name" : "b2",
        |          "dataType" : "long",
        |          "nullable" : true
        |        } ]
        |      }
        |    },
        |    "nullable" : true
        |  }, {
        |    "name" : "c",
        |    "dataType" : {
        |      "dataType" : "struct",
        |      "fields" : [ {
        |        "name" : "c1",
        |        "dataType" : "string",
        |        "nullable" : true
        |      }, {
        |        "name" : "c2",
        |        "dataType" : "long",
        |        "nullable" : true
        |      } ]
        |    },
        |    "nullable" : true
        |  } ]
        |""".stripMargin
    val expectedFields = JsonMethods.parse(StringInput(expectedFieldsJson)).values
    val actualFields = JsonMethods.parse(StringInput(actualOutput._2)).values
    assert(actualFields == expectedFields)
  }

  test("test main with includes and excludes") {
    val path = "target/schema"
    new File(path).delete()
    DataObjectSchemaExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getPath, "-p", path, "-i", "dataObjectCsv[0-9]", "-e", "dataObjectCsv5"))
    assert(new File(path).listFiles().map(_.getName.split('.').head).toSet == Set("dataObjectCsv1","dataObjectCsv2","dataObjectCsv3","dataObjectCsv4"))
  }
}