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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.exporter.ExportWriter.{formatSchema, parseSchema}
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class ExportWriterTest extends AnyFunSuite {

  private val tempDir = Files.createTempDirectory("exportwritertest")
  private val dataObjectId = DataObjectId("testDO")
  private val schema1 = SparkSchema(StructType(Seq(
    StructField("a", StringType),
    StructField("b", MapType(StringType, StringType)),
    StructField("c", ArrayType(StructType(Seq(
      StructField("c1", StringType),
      StructField("c2", BooleanType, nullable = false)
    ))))
  )))
  private val info1 = Some("test")

  test("writing and parsing schema") {
    val writer = FileExportWriter(tempDir.resolve("fileschemaexport1"))
    writer.writeSchema(formatSchema(Some(schema1), info1), dataObjectId, 100L)
    val actual = parseSchema(writer.readLatestSchema(dataObjectId).get)
    val expected = (schema1, info1)
    assert(actual === expected)
  }

  test("writing and parsing updated schema") {
    val schema2 = SparkSchema(schema1.inner.add(StructField("d", IntegerType)))
    val writer = FileExportWriter(tempDir.resolve("fileschemaexport2"))
    writer.writeSchema(formatSchema(Some(schema1), info1), dataObjectId, 100L)
    writer.writeSchema(formatSchema(Some(schema2), info1), dataObjectId, 105L)
    val actual = parseSchema(writer.readLatestSchema(dataObjectId).get)
    val expected = (schema2, info1)
    assert(actual === expected)
  }

  test("FileExportWriter lists, reads and writes versions") {
    val writer = FileExportWriter(tempDir.resolve("fileversions"))
    assert(writer.listVersions(ExportType.Stats, dataObjectId).isEmpty)
    writer.writeVersion(ExportType.Stats, "v300", dataObjectId, 300L)
    writer.writeVersion(ExportType.Stats, "v100", dataObjectId, 100L)
    writer.writeVersion(ExportType.Stats, "v200", dataObjectId, 200L)
    // writing an existing version is ignored
    writer.writeVersion(ExportType.Stats, "v200 again", dataObjectId, 200L)
    assert(writer.listVersions(ExportType.Stats, dataObjectId) == Seq(100L, 200L, 300L))
    assert(writer.readIndex(dataObjectId, "stats") == Seq("testDO.stats.100.json", "testDO.stats.200.json", "testDO.stats.300.json"))
    assert(writer.readVersion(ExportType.Stats, dataObjectId, 200L).contains("v200"))
    assert(writer.readVersion(ExportType.Stats, dataObjectId, 400L).isEmpty)
    assert(writer.readLatest(ExportType.Stats, dataObjectId).contains("v300"))
    assert(writer.listVersions(ExportType.Schema, dataObjectId).isEmpty)
  }

  test("HadoopExportWriter uses modification time as version") {
    val writer = HadoopExportWriter(new HadoopPath(tempDir.resolve("hadoopversions").toUri), new Configuration())
    assert(writer.listVersions(ExportType.Lineage, dataObjectId).isEmpty)
    val before = System.currentTimeMillis() / 1000
    writer.writeVersion(ExportType.Lineage, "lineage", dataObjectId, 100L)
    val versions = writer.listVersions(ExportType.Lineage, dataObjectId)
    assert(versions.size == 1 && versions.head >= before)
    assert(writer.readVersion(ExportType.Lineage, dataObjectId, versions.head).contains("lineage"))
    assert(writer.readLatest(ExportType.Lineage, dataObjectId).contains("lineage"))
    assert(!writer.keepsHistory)
  }
}
