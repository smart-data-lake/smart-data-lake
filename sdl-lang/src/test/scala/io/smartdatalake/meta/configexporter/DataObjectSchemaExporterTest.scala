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

import io.smartdatalake.config.ConfigToolbox
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.ColumnStatsType
import io.smartdatalake.testutils.DataFrameTestHelper.ComplexTypeTest
import io.smartdatalake.testutils.TestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject.HiveTableDataObject
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{Formats, NoTypeHints, StringInput}
import org.scalatest.{BeforeAndAfter, FunSuite}

import java.io.File
import java.nio.file.{Files, Paths}

class DataObjectSchemaExporterTest extends FunSuite with BeforeAndAfter {

  val configPath = getClass.getResource("/dagexporter/dagexporterTest.conf").getPath
  val exportPath = "./target/schema"

  before {
    FileUtils.deleteDirectory(new File(exportPath))
  }

  test("export simple schema") {
    val exporterConfig = DataObjectSchemaExporterConfig(Seq(configPath), includeRegex = "^(dataObjectCsv1)", exportPath = exportPath)
    DataObjectSchemaExporter.exportSchemas(exporterConfig)
    val actualOutput = DataObjectSchemaExporter.getLatestData("dataObjectCsv1", "schema", Paths.get(exporterConfig.exportPath))

    val expectedFieldsJson = """
      | {
      | "schema": [
      |  {
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
      |  }
      | ]}
      |""".stripMargin
    val expectedFields = JsonMethods.parse(StringInput(expectedFieldsJson)).values
    val actualFields = JsonMethods.parse(StringInput(actualOutput.get)).values
    assert(actualFields == expectedFields)
  }

  test("export complex schema") {
    val exporterConfig = DataObjectSchemaExporterConfig(Seq(configPath), includeRegex = "dataObjectParquet6", exportPath = exportPath)
    DataObjectSchemaExporter.exportSchemas(exporterConfig)
    val actualOutput = DataObjectSchemaExporter.getLatestData("dataObjectParquet6", "schema", Paths.get(exporterConfig.exportPath))
    val expectedFieldsJson =
      """ {
        | "schema": [
        |  {
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
        |  }
        | ]}
        |""".stripMargin
    val expectedFields = JsonMethods.parse(StringInput(expectedFieldsJson)).values
    val actualFields = JsonMethods.parse(StringInput(actualOutput.get)).values
    assert(actualFields == expectedFields)
  }

  test("test main with includes and excludes, dont update if same") {
    DataObjectSchemaExporter.main(Array("-c", configPath, "-p", exportPath, "-i", "dataObjectCsv[0-9]", "-e", "dataObjectCsv5"))
    assert(new File(exportPath).listFiles().filter(_.getName.endsWith(".json")).map(_.getName.split('.').head).toSet == Set("dataObjectCsv1","dataObjectCsv2","dataObjectCsv3","dataObjectCsv4"))
    assert(new File(exportPath).listFiles().filter(_.getName.endsWith(".index")).map(_.getName.split('.').head).toSet == Set("dataObjectCsv1","dataObjectCsv2","dataObjectCsv3","dataObjectCsv4"))
  }

  test("schema file is not updated if unchanged") {
    val dataObjectId = DataObjectId("test")
    val path = Paths.get("target/schemaUpdate")
    FileUtils.deleteDirectory(path.toFile)
    Files.createDirectories(path)
    // first write
    DataObjectSchemaExporter.writeSchemaIfChanged(DataObjectId("test"), Some(SparkSchema(StructType(Seq(StructField("a", StringType))))), None, path)
    assert(DataObjectSchemaExporter.readIndex(dataObjectId, "schema", path).length==1)
    Thread.sleep(1000)
    // second write -> no update
    DataObjectSchemaExporter.writeSchemaIfChanged(DataObjectId("test"), Some(SparkSchema(StructType(Seq(StructField("a", StringType))))), None, path)
    assert(DataObjectSchemaExporter.readIndex(dataObjectId, "schema", path).length == 1)
    Thread.sleep(1000)
    // third write -> update
    DataObjectSchemaExporter.writeSchemaIfChanged(DataObjectId("test"), Some(SparkSchema(StructType(Seq(StructField("a", IntegerType))))), None, path)
    assert(DataObjectSchemaExporter.readIndex(dataObjectId, "schema", path).length == 2)
  }

  test("export statistics") {
    val (registry, globalConfig) = ConfigToolbox.loadAndParseConfig(Seq(configPath))
    implicit val context: ActionPipelineContext = TestUtil.getDefaultActionPipelineContext(registry)
    val session = context.sparkSession
    import session.implicits._
    // prepare data object
    val hiveDO = registry.get[HiveTableDataObject](DataObjectId("dataObjectHive14"))
    hiveDO.dropTable
    val df = Seq(("ext", "doe", "john", ComplexTypeTest("a", 5)), ("ext", "smith", "peter", ComplexTypeTest("a", 3)), ("int", "emma", "brown", ComplexTypeTest("a", 7)))
      .toDF("type", "lastname", "firstname", "complex")
    hiveDO.writeSparkDataFrame(df, Seq())
    // export
    val exporterConfig = DataObjectSchemaExporterConfig(Seq(configPath), includeRegex = "dataObjectHive14", exportPath = "./target/schema")
    DataObjectSchemaExporter.exportSchemas(exporterConfig)
    // read stats and check
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val latestStats = DataObjectSchemaExporter.getLatestData(hiveDO.id, "stats", Paths.get(exporterConfig.exportPath))
      .map(Serialization.read[Map[String, Any]]).get
    assert(latestStats.apply("columns").asInstanceOf[Map[String,Any]]
      .apply("lastname").asInstanceOf[Map[String,Any]]
      .apply(ColumnStatsType.DistinctCount.toString) == 3)
  }
}