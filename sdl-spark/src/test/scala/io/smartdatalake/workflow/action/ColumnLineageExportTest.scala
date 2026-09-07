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
package io.smartdatalake.workflow.action

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, TestMode}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.spark.{MockSparkDataObject, SparkTestUtil}
import io.smartdatalake.workflow.action.generic.transformer.{SQLDfTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.action.spark.customlogic.CustomDfTransformer
import io.smartdatalake.workflow.action.spark.transformer.ScalaClassSparkDfTransformer
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Identity, Transformation}
import io.smartdatalake.workflow.dataframe.spark.SparkSubFeed
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}

/**
 * Test the column level lineage collected in the init phase of a "--test dry-run-with-lineage-export" run,
 * and its export, see issue #867.
 */
class ColumnLineageExportTest extends AnyFunSuite with BeforeAndAfterEach {

  protected implicit val session: SparkSession = SparkTestUtil.session
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry

  import session.implicits._

  private var tempDir: Path = _
  private var contextExec: ActionPipelineContext = _
  private var contextInitExport: ActionPipelineContext = _

  private def inputsOf(lineage: io.smartdatalake.workflow.dataframe.ColumnLineage, column: String): Seq[(String, String)] = {
    lineage.get(column).toSeq.flatMap(_.inputFields).map(f => (f.dataObjectId.id, f.column))
  }

  override def beforeEach(): Unit = {
    instanceRegistry.clear()
    instanceRegistry.register(SparkTestUtil.defaultSparkConnection)
    tempDir = Files.createTempDirectory("columnLineage")
    val context = SparkTestUtil.getDefaultActionPipelineContext
    contextExec = context.copy(phase = ExecutionPhase.Exec)
    contextInitExport = context.copy(
      appConfig = context.appConfig.copy(test = Some(TestMode.DryRunWithLineageExport)),
      globalConfig = context.globalConfig.copy(dataObjectsSchemaSource = Some(tempDir.toAbsolutePath.toString))
    )
  }

  test("the column lineage of an Action is collected in the init phase") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val tgtDO = MockSparkDataObject("tgt1").register
    val action = CopyAction("copyCities", srcDO.id, tgtDO.id,
      transformers = Seq(SQLDfTransformer(code = Some("select name, upper(country) as country, 'x' as constant from src1")))
    )
    instanceRegistry.register(action)

    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInitExport)

    val entry = contextInitExport.columnLineageExportRegistry.getColumnLineages.get(tgtDO.id)
    assert(entry.isDefined, "no column lineage was collected for export")
    assert(entry.get.actionId == action.id)
    val lineage = entry.get.lineage
    assert(lineage.get("name").get.inputFields.map(f => (f.dataObjectId.id, f.column, f.transformation.subtype))
      == Seq(("src1", "name", Identity)))
    assert(lineage.get("country").get.inputFields.map(f => (f.dataObjectId.id, f.column, f.transformation.subtype))
      == Seq(("src1", "country", Transformation)))
    // a constant column is reported without input columns, and is not counted as unresolved
    assert(lineage.get("constant").exists(_.inputFields.isEmpty))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("the column lineage of an Action with two inputs keeps both inputs apart") {
    val src1DO = MockSparkDataObject("src1").register
    src1DO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val src2DO = MockSparkDataObject("src2").register
    src2DO.writeSparkDataFrame(Seq(("CH", "Switzerland")).toDF("code", "label"), Seq(), isRecursiveInput = false, None)(contextExec)
    val tgtDO = MockSparkDataObject("tgt1").register
    val action = CustomDataFrameAction("joinCities", Seq(src1DO.id, src2DO.id), Seq(tgtDO.id),
      transformers = Seq(SQLDfsTransformer(code = Map(
        tgtDO.id.id -> "select src1.name, src2.label as countryName from src1 join src2 on src1.country = src2.code"
      )))
    )
    instanceRegistry.register(action)

    action.init(Seq(SparkSubFeed(None, src1DO.id, Seq()), SparkSubFeed(None, src2DO.id, Seq())))(contextInitExport)

    val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
    assert(lineage.get("name").get.inputFields.map(f => (f.dataObjectId.id, f.column)) == Seq(("src1", "name")))
    assert(lineage.get("countryName").get.inputFields.map(f => (f.dataObjectId.id, f.column)) == Seq(("src2", "label")))
  }

  test("the collected column lineage is exported as OpenLineage column lineage facet") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val tgtDO = MockSparkDataObject("tgt1").register
    val action = CopyAction("copyCities", srcDO.id, tgtDO.id,
      transformers = Seq(SQLDfTransformer(code = Some("select upper(name) as city from src1")))
    )
    instanceRegistry.register(action)
    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInitExport)

    DefaultSmartDataLakeBuilder.exportColumnLineage(contextInitExport)

    val exportedFiles = Files.list(tempDir).toArray.map(_.toString).filter(_.endsWith(".lineage.json"))
    assert(exportedFiles.length == 1, s"expected one exported lineage file, got ${exportedFiles.mkString(", ")}")
    val document = Files.readString(Path.of(exportedFiles.head))
    val json = org.json4s.jackson.JsonMethods.parse(document)
    implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
    assert((json \ "actionId").extract[String] == "copyCities")
    assert((json \ "dataObjectId").extract[String] == "tgt1")
    val inputField = (json \ "columnLineage" \ "fields" \ "city" \ "inputFields") (0)
    assert((inputField \ "namespace").extract[String] == "sdlb")
    assert((inputField \ "name").extract[String] == "src1")
    assert((inputField \ "field").extract[String] == "name")
    val transformation = (inputField \ "transformations") (0)
    assert((transformation \ "type").extract[String] == "DIRECT")
    assert((transformation \ "subtype").extract[String] == "TRANSFORMATION")
    assert((transformation \ "description").extract[String].contains("upper"))
    assert(!(transformation \ "masking").extract[Boolean])
  }

  test("the lineage of chained Actions stops at the intermediate DataObject although the DataFrame is cached") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val interDO = MockSparkDataObject("inter1").register
    val tgtDO = MockSparkDataObject("tgt1").register
    val action1 = CopyAction("a1", srcDO.id, interDO.id, cacheOutput = true,
      transformers = Seq(SQLDfTransformer(code = Some("select upper(name) as city, country from src1")))
    )
    val action2 = CopyAction("a2", interDO.id, tgtDO.id,
      transformers = Seq(SQLDfTransformer(code = Some("select concat(city, '-', country) as label from inter1")))
    )
    instanceRegistry.register(action1)
    instanceRegistry.register(action2)

    val interSubFeeds = action1.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInitExport)
    // with cacheOutput the DataFrame is handed over to the next Action, so its plan cumulates over both Actions
    assert(interSubFeeds.head.asInstanceOf[SparkSubFeed].dataFrame.isDefined)
    action2.init(interSubFeeds)(contextInitExport)

    val lineages = contextInitExport.columnLineageExportRegistry.getColumnLineages
    assert(inputsOf(lineages(interDO.id).lineage, "city") == Seq(("src1", "name")))
    // the lineage of the second Action must stop at inter1 and not reach through to src1
    assert(inputsOf(lineages(tgtDO.id).lineage, "label") == Seq(("inter1", "city"), ("inter1", "country")))
    assert(lineages(tgtDO.id).lineage.unresolvedColumns.isEmpty)
  }

  test("reading a cached output and its own input in one Action reports shared columns for both inputs") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val interDO = MockSparkDataObject("inter1").register
    val tgtDO = MockSparkDataObject("tgt1").register
    // action1 passes its input through unchanged and caches it, so inter1 has the columns of src1
    val action1 = CopyAction("a1", srcDO.id, interDO.id, cacheOutput = true)
    val action2 = CustomDataFrameAction("a2", Seq(interDO.id, srcDO.id), Seq(tgtDO.id),
      transformers = Seq(SQLDfsTransformer(code = Map(tgtDO.id.id ->
        "select i.name as fromInter, s.country as fromSrc from inter1 i join src1 s on i.name = s.name"
      )))
    )
    instanceRegistry.register(action1)
    instanceRegistry.register(action2)

    val interSubFeeds = action1.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInitExport)
    action2.init(interSubFeeds :+ SparkSubFeed(None, srcDO.id, Seq()))(contextInitExport)

    val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
    // the column belongs to both inputs, as inter1 is a copy of src1 which was not materialized in between
    assert(inputsOf(lineage, "fromInter") == Seq(("inter1", "name"), ("src1", "name")))
    // Spark's analyzer replaces the expression ids of one side of a join reading the same columns twice, so
    // the columns of that side can not be traced back. They are reported as unresolved and not as a wrong source.
    assert(lineage.unresolvedColumns == Seq("fromSrc"))
  }

  test("the column lineage of an Action using a user defined function is collected") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val tgtDO = MockSparkDataObject("tgt1").register
    val action = CopyAction("udfLineage", srcDO.id, tgtDO.id,
      transformers = Seq(ScalaClassSparkDfTransformer(className = classOf[ColumnLineageTestGeoUdfTransformer].getName))
    )
    instanceRegistry.register(action)

    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInitExport)

    val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
    // the Action adds the column comments from the ScalaDoc of the case class returned by the UDF before the
    // lineage is collected, see issue #765. This rewrites the output attributes of the plan.
    assert(inputsOf(lineage, "geo") == Seq(("src1", "name")))
    assert(inputsOf(lineage, "name") == Seq(("src1", "name")))
    assert(lineage.unresolvedColumns.isEmpty)
  }

  test("no column lineage is collected without the lineage export test mode") {
    val srcDO = MockSparkDataObject("src1").register
    srcDO.writeSparkDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
    val tgtDO = MockSparkDataObject("tgt1").register
    val action = CopyAction("copyCities", srcDO.id, tgtDO.id)
    instanceRegistry.register(action)
    val contextInit = contextInitExport.copy(appConfig = contextInitExport.appConfig.copy(test = None))

    action.init(Seq(SparkSubFeed(None, srcDO.id, Seq())))(contextInit)

    assert(contextInit.columnLineageExportRegistry.isEmpty)
  }
}

/**
 * A geo location enriched from a city name.
 *
 * @param lat Latitude in decimal degrees, WGS84.
 * @param lon Longitude in decimal degrees, WGS84.
 */
case class ColumnLineageTestGeo(lat: Double, lon: Double)

/**
 * Adds a geo location computed by a user defined function returning a case class.
 */
class ColumnLineageTestGeoUdfTransformer extends CustomDfTransformer {
  override def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String): DataFrame = {
    val geoUdf = udf((city: String) => ColumnLineageTestGeo(1.0, 2.0))
    df.withColumn("geo", geoUdf(col("name")))
  }
}
