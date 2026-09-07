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
package io.smartdatalake.testutils

import io.smartdatalake.app.{DefaultSmartDataLakeBuilder, TestMode}
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.action.generic.customlogic.{CustomGenericDfTransformer, CustomGenericDfsTransformer}
import io.smartdatalake.workflow.action.generic.transformer.{ScalaClassGenericDfTransformer, ScalaClassGenericDfsTransformer}
import io.smartdatalake.workflow.action.{CopyAction, CustomDataFrameAction}
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataframe.ColumnTransformation.{Identity, Transformation}
import io.smartdatalake.workflow.dataframe.{ColumnLineage, DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion, ExecutionPhase}

import java.nio.file.{Files, Path}
import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for the column level lineage collected in the init phase of a
 * "--test dry-run-with-lineage-export" run, and for its export, see issue #867.
 *
 * These tests cover the engine independent part: the registry filled while an Action is initialized, the
 * Action boundaries of the lineage, and the exported OpenLineage document. Transformations which only one
 * engine can express, e.g. a SQL transformer or a user defined function, are tested in the engine specific
 * suites.
 */
trait ColumnLineageExportBehaviour {

  def subFeedType: Type

  def defaultEngineConnection: Connection with EngineConnection

  /**
   * Create a transactional mock DataObject serving DataFrames of the engine under test.
   */
  def createDataObject(name: String)(implicit instanceRegistry: InstanceRegistry): DataObject with CanCreateDataFrame with CanWriteDataFrame

  /**
   * The default context of the engine under test, e.g. holding its Spark session.
   */
  def defaultActionPipelineContext(implicit instanceRegistry: InstanceRegistry): ActionPipelineContext

  /**
   * An input SubFeed without a DataFrame, as an Action gets it before reading its input DataObject.
   */
  def inputSubFeed(dataObjectId: DataObjectId): DataFrameSubFeed

  protected lazy val helper: DataFrameSubFeedCompanion = DataFrameSubFeed.getCompanion(subFeedType)

  import helper.implicits._

  /**
   * Get the lineage of a column as tuples of input DataObject, input column and transformation subtype.
   */
  protected def inputsOf(lineage: ColumnLineage, column: String): Seq[(String, String, String)] = {
    lineage.get(column).toSeq.flatMap(_.inputFields).map(f => (f.dataObjectId.id, f.column, f.transformation.subtype))
  }

  /**
   * Set up a registry with a temporary export directory, an exec-phase context to fill the mock DataObjects
   * and an init-phase context running with the lineage export test mode.
   */
  protected def withLineageExport(test: (InstanceRegistry, ActionPipelineContext, ActionPipelineContext, Path) => Unit): Unit = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(defaultEngineConnection)
    val tempDir = Files.createTempDirectory("columnLineage")
    val context = defaultActionPipelineContext
    val contextExec = context.copy(phase = ExecutionPhase.Exec)
    val contextInitExport = context.copy(
      appConfig = context.appConfig.copy(test = Some(TestMode.DryRunWithLineageExport)),
      globalConfig = context.globalConfig.copy(dataObjectsSchemaSource = Some(tempDir.toAbsolutePath.toString))
    )
    test(instanceRegistry, contextExec, contextInitExport, tempDir)
  }

  def testTheColumnLineageOfAnActionIsCollectedInTheInitPhase(): Unit = withLineageExport {
    (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withLineageExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("copyCities", srcDO.id, tgtDO.id,
        transformers = Seq(ScalaClassGenericDfTransformer(className = classOf[ColumnLineageTestTransformer].getName))
      )
      instanceRegistry.register(action)

      action.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)

      val entry = contextInitExport.columnLineageExportRegistry.getColumnLineages.get(tgtDO.id)
      assert(entry.isDefined, "no column lineage was collected for export")
      assert(entry.get.actionId == action.id)
      val lineage = entry.get.lineage
      assert(inputsOf(lineage, "city") == Seq(("src1", "name", Identity)))
      assert(inputsOf(lineage, "country") == Seq(("src1", "country", Transformation)))
      // a constant column is reported without input columns, and is not counted as unresolved
      assert(lineage.get("constant").exists(_.inputFields.isEmpty))
      assert(lineage.unresolvedColumns.isEmpty)
  }

  def testTheColumnLineageOfAnActionWithTwoInputsKeepsBothInputsApart(): Unit = withLineageExport {
    (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withLineageExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val src1DO = createDataObject("src1")
      src1DO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val src2DO = createDataObject("src2")
      src2DO.writeDataFrame(Seq(("CH", "Switzerland")).toDF("code", "label"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CustomDataFrameAction("joinCities", Seq(src1DO.id, src2DO.id), Seq(tgtDO.id),
        transformers = Seq(ScalaClassGenericDfsTransformer(className = classOf[ColumnLineageTestJoinTransformer].getName))
      )
      instanceRegistry.register(action)

      action.init(Seq(inputSubFeed(src1DO.id), inputSubFeed(src2DO.id)))(contextInitExport)

      val lineage = contextInitExport.columnLineageExportRegistry.getColumnLineages(tgtDO.id).lineage
      assert(inputsOf(lineage, "name") == Seq(("src1", "name", Identity)))
      assert(inputsOf(lineage, "countryName") == Seq(("src2", "label", Identity)))
  }

  def testTheCollectedColumnLineageIsExportedAsOpenLineageColumnLineageFacet(): Unit = withLineageExport {
    (instanceRegistry, contextExec, contextInitExport, tempDir) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withLineageExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("copyCities", srcDO.id, tgtDO.id,
        transformers = Seq(ScalaClassGenericDfTransformer(className = classOf[ColumnLineageTestTransformer].getName))
      )
      instanceRegistry.register(action)
      action.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)

      DefaultSmartDataLakeBuilder.exportColumnLineage(contextInitExport)

      val exportedFiles = Files.list(tempDir).toArray.map(_.toString).filter(_.endsWith(".lineage.json"))
      assert(exportedFiles.length == 1, s"expected one exported lineage file, got ${exportedFiles.mkString(", ")}")
      val json = org.json4s.jackson.JsonMethods.parse(Files.readString(Path.of(exportedFiles.head)))
      implicit val formats: org.json4s.Formats = org.json4s.DefaultFormats
      assert((json \ "actionId").extract[String] == "copyCities")
      assert((json \ "dataObjectId").extract[String] == "tgt1")
      val inputField = (json \ "columnLineage" \ "fields" \ "country" \ "inputFields") (0)
      assert((inputField \ "namespace").extract[String] == "sdlb")
      assert((inputField \ "name").extract[String] == "src1")
      assert((inputField \ "field").extract[String] == "country")
      val transformation = (inputField \ "transformations") (0)
      assert((transformation \ "type").extract[String] == "DIRECT")
      assert((transformation \ "subtype").extract[String] == "TRANSFORMATION")
      assert((transformation \ "description").extract[String].contains("concat"))
      assert(!(transformation \ "masking").extract[Boolean])
  }

  def testTheLineageOfChainedActionsStopsAtTheIntermediateDataObject(): Unit = withLineageExport {
    (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withLineageExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val interDO = createDataObject("inter1")
      val tgtDO = createDataObject("tgt1")
      val action1 = CopyAction("a1", srcDO.id, interDO.id, cacheOutput = true,
        transformers = Seq(ScalaClassGenericDfTransformer(className = classOf[ColumnLineageTestTransformer].getName))
      )
      val action2 = CopyAction("a2", interDO.id, tgtDO.id,
        transformers = Seq(ScalaClassGenericDfTransformer(className = classOf[ColumnLineageTestLabelTransformer].getName))
      )
      instanceRegistry.register(action1)
      instanceRegistry.register(action2)

      val interSubFeeds = action1.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)
      // with cacheOutput the DataFrame is handed over to the next Action, so it cumulates over both Actions
      assert(interSubFeeds.head.asInstanceOf[DataFrameSubFeed].dataFrame.isDefined)
      action2.init(interSubFeeds)(contextInitExport)

      val lineages = contextInitExport.columnLineageExportRegistry.getColumnLineages
      assert(inputsOf(lineages(interDO.id).lineage, "city") == Seq(("src1", "name", Identity)))
      // the lineage of the second Action must stop at inter1 and not reach through to src1
      assert(inputsOf(lineages(tgtDO.id).lineage, "label")
        == Seq(("inter1", "city", Transformation), ("inter1", "country", Transformation)))
      assert(lineages(tgtDO.id).lineage.unresolvedColumns.isEmpty)
  }

  def testNoColumnLineageIsCollectedWithoutTheLineageExportTestMode(): Unit = withLineageExport {
    (instanceRegistry, contextExec, contextInitExport, _) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withLineageExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("copyCities", srcDO.id, tgtDO.id)
      instanceRegistry.register(action)
      val contextInit = contextInitExport.copy(appConfig = contextInitExport.appConfig.copy(test = None))

      action.init(Seq(inputSubFeed(srcDO.id)))(contextInit)

      assert(contextInit.columnLineageExportRegistry.isEmpty)
  }
}

/**
 * Renames a column, calculates one and adds a constant, to get one of each kind of lineage.
 */
class ColumnLineageTestTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.select(Seq(col("name").as("city"), concat(col("country"), lit("!")).as("country"), lit("x").as("constant")))
  }
}

/**
 * Combines the columns of the intermediate DataObject of the chained Actions test into one column.
 */
class ColumnLineageTestLabelTransformer extends CustomGenericDfTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import helper._
    df.select(Seq(concat(col("city"), lit("-"), col("country")).as("label")))
  }
}

/**
 * Joins the two input DataObjects, taking one column from each of them.
 */
class ColumnLineageTestJoinTransformer extends CustomGenericDfsTransformer {
  override def transform(helper: DataFrameFunctions, options: Map[String, String], dfs: Map[String, GenericDataFrame]): Map[String, GenericDataFrame] = {
    import helper._
    val cities = dfs("src1").as("c")
    val countries = dfs("src2").as("n")
    Map("tgt1" -> cities.join(countries, col("c.country") === col("n.code"), "inner")
      .select(Seq(col("c.name"), col("n.label").as("countryName"))))
  }
}
