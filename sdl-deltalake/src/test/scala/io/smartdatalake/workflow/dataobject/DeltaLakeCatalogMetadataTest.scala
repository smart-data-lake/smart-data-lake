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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.util.hdfs.{HdfsUtil, SparkHdfsUtil}
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.dataobject.DeltaLakeTestUtils.deltaDb
import io.smartdatalake.workflow.dataobject.generic.{CatalogMetadataApplier, Table}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file
import java.nio.file.Files

/**
 * Test applying table metadata to the catalog at deployment time, see issues #1121 and #1127.
 *
 * Note that no catalog metadata is written during a normal SDLB run anymore. It is applied by
 * DataObjectSchemaExporter, which uses [[CatalogMetadataApplier]].
 */
class DeltaLakeCatalogMetadataTest extends AnyFunSuite with BeforeAndAfterAll {

  protected implicit val session: SparkSession = DeltaLakeTestUtils.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val context: ActionPipelineContext =
    SparkTestUtil.getDefaultActionPipelineContext.copy(phase = ExecutionPhase.Exec)

  val tempDir: file.Path = Files.createTempDirectory("catalogMetadata")
  val tempPath: String = tempDir.toAbsolutePath.toString

  override def beforeAll(): Unit = {
    val warehousePath = new Path("spark-warehouse/delta.db")
    implicit val fs: FileSystem = SparkHdfsUtil.getHadoopFsFromSpark(warehousePath)(session)
    HdfsUtil.deletePath(path = warehousePath, doWarn = false)
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  /**
   * The schema a dry-run with schema export would have written, carrying the column comments.
   */
  private def exportedSchema = SparkSchema(StructType(Seq(
    StructField("city", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "Name of the city").build())
  )))

  private def createTable(name: String, description: Option[String]): DeltaLakeTableDataObject = {
    val table = Table(db = Some(deltaDb), name = name)
    val dataObject = DeltaLakeTableDataObject("tgt_" + name, path = Some(s"$tempPath/${table.fullName}"),
      table = table, metadata = description.map(d => DataObjectMetadata(description = Some(d))))
    instanceRegistry.register(dataObject)
    dataObject.dropTable
    val helper = DataFrameSubFeed.getCompanion(dataObject.getSubFeedSupportedTypes.head)
    import helper.implicits._
    val df = Seq(("Bern", 1), ("Zurich", 2)).toDF("city", "nr")
    dataObject.writeDataFrame(df, Seq())
    dataObject
  }

  test("metadata.description works without table.catalog on open source spark") {
    // issue #1127: this used to fail in the prepare phase with a require on table.catalog,
    // and with a PARSE_SYNTAX_ERROR on "USE CATALOG" once the catalog was set.
    val dataObject = createTable("no_catalog", Some("A table without a catalog"))
    assert(dataObject.table.catalog.isEmpty)
    dataObject.prepare
    dataObject.setTableComment("A table without a catalog")
    assert(dataObject.getTableComment.contains("A table without a catalog"))
  }

  test("apply writes table and column comments, and is idempotent") {
    val dataObject = createTable("apply_meta", Some("Cities of interest"))
    val applier = new CatalogMetadataApplier(_ => Some(exportedSchema))

    // plan reports the changes to be applied
    val changes = applier.plan(dataObject)
    assert(changes.isDefined)
    assert(changes.get.tableComment.contains("Cities of interest"))
    assert(changes.get.columnComments == Map(Seq("city") -> "Name of the city"))

    // apply writes them to the catalog
    applier.apply(dataObject, changes.get)
    assert(dataObject.getTableComment.contains("Cities of interest"))
    assert(dataObject.getColumnComments == Map(Seq("city") -> "Name of the city"))

    // issue #1121: applying again must not write anything
    val changesAfterApply = applier.plan(dataObject)
    assert(changesAfterApply.exists(_.isEmpty), s"expected no changes, got ${changesAfterApply.map(_.describe)}")
  }

  test("comments containing a single quote are escaped") {
    val dataObject = createTable("escaping", Some("Bern's cities"))
    val applier = new CatalogMetadataApplier(_ => None)
    val changes = applier.plan(dataObject)
    applier.apply(dataObject, changes.get)
    assert(dataObject.getTableComment.contains("Bern's cities"))
  }

  test("no metadata is written to the catalog during a normal run") {
    val dataObject = createTable("no_run_write", Some("Not written during the run"))
    // writing the DataFrame and postWrite must not set the table comment anymore
    dataObject.postWrite(Seq())
    assert(dataObject.getTableComment.isEmpty)
  }
}
