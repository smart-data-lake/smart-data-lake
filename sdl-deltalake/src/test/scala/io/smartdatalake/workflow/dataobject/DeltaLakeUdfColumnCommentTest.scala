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

import io.smartdatalake.app.TestMode
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.util.hdfs.{HdfsUtil, SparkHdfsUtil}
import io.smartdatalake.util.spark.SparkSchemaUtil
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.dataframe.GenericSchemaUtil
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.dataobject.DeltaLakeTestUtils.deltaDb
import io.smartdatalake.workflow.dataobject.generic.Table
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file
import java.nio.file.Files

/**
 * Test that column comments derived from the ScalaDoc of a case class returned by a user defined function are
 * persisted in a DeltaLake table, and are therefore available for the SDLB UI. See issue #765.
 */
class DeltaLakeUdfColumnCommentTest extends AnyFunSuite with BeforeAndAfterAll {

  protected implicit val session: SparkSession = DeltaLakeTestUtils.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
  implicit val contextInit: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext
  val contextExec: ActionPipelineContext = contextInit.copy(phase = ExecutionPhase.Exec)
  // init phase context of a "--test dry-run-with-schema-export" run, which collects the schemas to export
  val contextInitExport: ActionPipelineContext =
    contextInit.copy(appConfig = contextInit.appConfig.copy(test = Some(TestMode.DryRunWithSchemaExport)))

  import session.implicits._

  val tempDir: file.Path = Files.createTempDirectory("udfComments")
  val tempPath: String = tempDir.toAbsolutePath.toString

  override def beforeAll(): Unit = {
    val warehousePath = new Path("spark-warehouse/delta.db")
    implicit val fs: FileSystem = SparkHdfsUtil.getHadoopFsFromSpark(warehousePath)(session)
    HdfsUtil.deletePath(path = warehousePath, doWarn = false)
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
  }

  test("udf scaladoc column comments are persisted in the delta table") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq("Bern", "Zurich").toDF("city"), Seq(), isRecursiveInput = false, None)(contextExec)

    val table = Table(db = Some(deltaDb), name = "udf_comments")
    val tgtDO = DeltaLakeTableDataObject("tgt1", path = Some(tempPath + s"/${table.fullName}"), table = table)
    instanceRegistry.register(tgtDO)
    tgtDO.dropTable

    val transformer = ScalaClassSparkDfTransformer(className = classOf[DeltaTestGeoUdfTransformer].getName)
    val action = CopyAction("udfComments", srcDO.id, tgtDO.id, transformers = Seq(transformer))
    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInitExport)
    action.exec(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextExec)

    // the init phase schema is what a dry-run exports for the UI and for DataObjectSchemaExporter
    val exportedSchema = contextInitExport.schemaExportRegistry.getSchemas.get(tgtDO.id)
    assert(exportedSchema.isDefined, "no schema was collected for export")
    val exportedComments = GenericSchemaUtil.columnComments(exportedSchema.get)
      .map { case (path, c) => path.mkString(".") -> c }
    assert(exportedComments.get("geo").contains("A geo location enriched from an address."))
    assert(exportedComments.get("geo.lat").contains("Latitude in decimal degrees, WGS84."))
    assert(exportedComments.get("geo.lon").contains("Longitude in decimal degrees, WGS84."))
    assert(exportedComments.get("geo.tags.key").contains("The tag key."))

    // a newly created table persists the comments of the written schema, no catalog DDL is needed for that
    val tableSchema = session.table(table.fullName).schema
    val comments = SparkSchemaUtil.columnsComments(tableSchema).map { case (path, c) => path.mkString(".") -> c }
    assert(comments.get("geo").contains("A geo location enriched from an address."))
    assert(comments.get("geo.lat").contains("Latitude in decimal degrees, WGS84."))
    assert(comments.get("geo.lon").contains("Longitude in decimal degrees, WGS84."))
    assert(comments.get("geo.tags.key").contains("The tag key."))

    // data must be written correctly
    assert(session.table(table.fullName).select("city").as[String].collect().toSet == Set("Bern", "Zurich"))
  }
}

/**
 * A geo location enriched from an address.
 *
 * @param lat  Latitude in decimal degrees, WGS84.
 * @param lon  Longitude in decimal degrees, WGS84.
 * @param tags Free-form tags attached to the location.
 */
case class DeltaTestGeo(lat: Double, lon: Double, tags: Seq[DeltaTestTag])

/**
 * A tag attached to a geo location.
 *
 * @param key   The tag key.
 * @param value The tag value.
 */
case class DeltaTestTag(key: String, value: String)

/**
 * Adds a geo location computed by a user defined function returning a case class.
 */
class DeltaTestGeoUdfTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    val geoUdf = udf((city: String) => DeltaTestGeo(1.0, 2.0, Seq(DeltaTestTag("key1", "value1"))))
    df.withColumn("geo", geoUdf(col("city")))
  }
}
