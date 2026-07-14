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

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.workflow.action.CustomDataFrameAction
import io.smartdatalake.workflow.action.generic.transformer.{DebugTransformer, DfTransformerWrapperDfsTransformer, SQLDfsTransformer}
import io.smartdatalake.workflow.dataframe.plainScala.ScalaSubFeed
import io.smartdatalake.workflow.dataobject.generic.{CanCreateDataFrame, CanWriteDataFrame, TableDataObject}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase}

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[DebugTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * Note: not portable to ScalaSubFeed today: this test combines src1/src2 via a [[SQLDfsTransformer]], which uses
 * `DataFrameFunctions.sql`, not implemented for ScalaSubFeed. `DebugTransformer` itself is fully generic.
 *
 * Note also, like [[DeduplicateActionBehaviour]], the input placeholder subfeeds are built with [[ScalaSubFeed]]
 * regardless of the engine under test: they carry no DataFrame, only the DataObject id, and the Action re-reads
 * the actual DataFrame from the DataObject itself.
 */
trait DebugTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testCopyLoadWithTransformerRegularAndSkippedInput(
      createSrcDataObject: String => TableDataObject with CanCreateDataFrame with CanWriteDataFrame,
      createTgtDataObject: String => TableDataObject with CanCreateDataFrame with CanWriteDataFrame
  ): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    // setup DataObjects
    val srcDO1 = createSrcDataObject("src1")
    instanceRegistry.register(srcDO1)
    val srcDO2 = createSrcDataObject("src2")
    instanceRegistry.register(srcDO2)
    val tgtDO1 = createTgtDataObject("tgt1")
    instanceRegistry.register(tgtDO1)

    // prepare
    val customTransformerConfig = SQLDfsTransformer(code = Map(tgtDO1.id.id -> "select * from src1 union all select * from src2"))
    val debugDfTransformer = DebugTransformer(show = true, showOptions = Map("vertical" -> "true"), explain = true,
      explainOptions = Map("mode" -> "extended"))
    val debugDfsTransformer = DfTransformerWrapperDfsTransformer(transformer = debugDfTransformer, subFeedsToApply = Seq("src1"))
    val l1 = Seq(("jonson", "rob", 5)).toDF("lastname", "firstname", "rating")
    srcDO1.writeDataFrame(l1, Seq())
    val l2 = Seq(("doe", "bob", 3)).toDF("lastname", "firstname", "rating")
    srcDO2.writeDataFrame(l2, Seq())

    // execute - we can just check that there are no exceptions, but looking for the logs is difficult
    val action1 = CustomDataFrameAction("ca", List(srcDO1.id, srcDO2.id), List(tgtDO1.id),
      transformers = Seq(customTransformerConfig, debugDfsTransformer))
    instanceRegistry.register(action1)
    val srcSubFeed1 = ScalaSubFeed(None, "src1", Seq())
    val srcSubFeed2 = ScalaSubFeed(None, "src2", Seq())
    val contextExec = context.copy(phase = ExecutionPhase.Exec)
    action1.preInit(Seq(srcSubFeed1, srcSubFeed2), Seq())
    action1.preExec(Seq(srcSubFeed1, srcSubFeed2))(contextExec)
    action1.exec(Seq(srcSubFeed1, srcSubFeed2))(contextExec)
  }
}
