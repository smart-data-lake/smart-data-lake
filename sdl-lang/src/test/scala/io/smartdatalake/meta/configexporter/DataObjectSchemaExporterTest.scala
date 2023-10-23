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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import org.apache.commons.io.FileUtils
import org.json4s.StringInput
import org.json4s.jackson.JsonMethods
import org.scalatest.FunSuite

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.io.Source
import scala.util.Using

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

  test("test main with includes and excludes, dont update if same") {
    val path = "target/schema"
    FileUtils.deleteDirectory(new File(path))
    DataObjectSchemaExporter.main(Array("-c", getClass.getResource("/dagexporter/dagexporterTest.conf").getPath, "-p", path, "-i", "dataObjectCsv[0-9]", "-e", "dataObjectCsv5"))
    assert(new File(path).listFiles().filter(_.getName.endsWith(".json")).map(_.getName.split('.').head).toSet == Set("dataObjectCsv1","dataObjectCsv2","dataObjectCsv3","dataObjectCsv4"))
    assert(new File(path).listFiles().filter(_.getName.endsWith(".index")).map(_.getName.split('.').head).toSet == Set("dataObjectCsv1","dataObjectCsv2","dataObjectCsv3","dataObjectCsv4"))
  }

  test("schema file is not updated if unchanged") {
    val dataObjectId = DataObjectId("test")
    val path = Paths.get("target/schemaUpdate")
    FileUtils.deleteDirectory(path.toFile)
    Files.createDirectories(path)
    // first write
    DataObjectSchemaExporter.writeSchemaIfChanged(DataObjectId("test"), "abc", path)
    assert(readIndex(dataObjectId, path).length==1)
    Thread.sleep(1000)
    // second write -> no update
    DataObjectSchemaExporter.writeSchemaIfChanged(DataObjectId("test"), "abc", path)
    assert(readIndex(dataObjectId, path).length == 1)
    Thread.sleep(1000)
    // third write -> update
    DataObjectSchemaExporter.writeSchemaIfChanged(DataObjectId("test"), "abcd", path)
    assert(readIndex(dataObjectId, path).length == 2)
  }

  def readIndex(dataObjectId: DataObjectId, path: Path): Seq[String] = {
    Using(Source.fromFile(path.resolve(s"${dataObjectId.id}.index").toFile)) {
      _.getLines().toList.filter(_.trim.nonEmpty)
    }.get
  }
}