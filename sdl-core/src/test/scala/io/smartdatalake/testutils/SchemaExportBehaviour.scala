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

import io.smartdatalake.app.TestMode
import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.action.CopyAction
import io.smartdatalake.workflow.action.generic.transformer.ScalaClassGenericDfTransformer
import io.smartdatalake.workflow.connection.{Connection, EngineConnection}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, DataFrameSubFeedCompanion, ExecutionPhase}

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for the schemas collected in the init phase of a "--test dry-run-with-schema-export" run.
 *
 * The schema of a DataObject is collected as output of the DataFrame-Action writing it. Input DataObjects which
 * no DataFrame-Action writes, e.g. files delivered by a FileTransferAction, are collected when an Action reads them.
 */
trait SchemaExportBehaviour {

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
   * Set up a registry, an exec-phase context to fill the mock DataObjects and an init-phase context running
   * with the given test mode.
   */
  protected def withSchemaExport(testMode: TestMode.Value = TestMode.DryRunWithSchemaExport)
                                (test: (InstanceRegistry, ActionPipelineContext, ActionPipelineContext) => Unit): Unit = {
    implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry
    instanceRegistry.register(defaultEngineConnection)
    val context = defaultActionPipelineContext
    val contextExec = context.copy(phase = ExecutionPhase.Exec)
    val contextInitExport = context.copy(appConfig = context.appConfig.copy(test = Some(testMode)))
    test(instanceRegistry, contextExec, contextInitExport)
  }

  protected def exportedColumns(context: ActionPipelineContext, dataObjectId: DataObjectId): Option[Seq[String]] =
    context.schemaExportRegistry.getSchemas.get(dataObjectId).map(_.columns)

  def testTheSchemaOfAnInputOnlyDataObjectIsCollected(): Unit = withSchemaExport() {
    (instanceRegistry, contextExec, contextInitExport) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withSchemaExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("copyCities", srcDO.id, tgtDO.id,
        transformers = Seq(ScalaClassGenericDfTransformer(className = classOf[ColumnLineageTestTransformer].getName))
      )
      instanceRegistry.register(action)

      action.init(Seq(inputSubFeed(srcDO.id)))(contextInitExport)

      // the input is not written by any DataFrame-Action, its schema is collected from reading it
      assert(exportedColumns(contextInitExport, srcDO.id).contains(Seq("name", "country")))
      // the output schema is collected as before
      assert(exportedColumns(contextInitExport, tgtDO.id).contains(Seq("city", "country", "constant")))
  }

  def testTheSchemaOfADataObjectWrittenByADataFrameActionIsNotCollectedAsInput(): Unit = withSchemaExport() {
    (instanceRegistry, contextExec, contextInitExport) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withSchemaExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      val intDO = createDataObject("int1")
      intDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action1 = CopyAction("writeInt", srcDO.id, intDO.id)
      val action2 = CopyAction("readInt", intDO.id, tgtDO.id)
      instanceRegistry.register(Seq(action1, action2))

      // only the second Action is selected, e.g. by the feed selector
      action2.init(Seq(inputSubFeed(intDO.id)))(contextInitExport)

      // the schema of int1 is defined by action1, and exported only if action1 is part of the dry-run
      assert(exportedColumns(contextInitExport, intDO.id).isEmpty)
      assert(exportedColumns(contextInitExport, tgtDO.id).contains(Seq("name", "country")))
  }

  def testNoInputSchemaIsCollectedWithoutTheSchemaExportTestMode(): Unit = withSchemaExport(TestMode.DryRun) {
    (instanceRegistry, contextExec, contextInit) =>
      implicit val registry: InstanceRegistry = instanceRegistry
      // the DataFrames of the mock DataObjects are created in the exec phase, see withSchemaExport
      implicit val dataFrameContext: ActionPipelineContext = contextExec
      val srcDO = createDataObject("src1")
      srcDO.writeDataFrame(Seq(("Bern", "CH")).toDF("name", "country"), Seq(), isRecursiveInput = false, None)(contextExec)
      val tgtDO = createDataObject("tgt1")
      val action = CopyAction("copyCities", srcDO.id, tgtDO.id)
      instanceRegistry.register(action)

      action.init(Seq(inputSubFeed(srcDO.id)))(contextInit)

      assert(contextInit.schemaExportRegistry.isEmpty)
  }
}
