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
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.CustomDataFrameAction
import io.smartdatalake.workflow.action.generic.transformer.SQLDfsTransformer
import io.smartdatalake.workflow.dataobject.DataObject

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[SQLDfsTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 *
 * Note: not portable to ScalaSubFeed today: `DataFrameFunctions.sql` is not implemented for ScalaSubFeed
 * (same known gap as documented for `DeduplicateActionBehaviour`'s SQLDfTransformer-based test).
 */
trait SQLDfsTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  // SQLDfsTransformer looks up the actionId's output DataObject in the registry (only used to determine connection info)
  private def registerAction(createDataObject: String => DataObject): Unit = {
    instanceRegistry.register(createDataObject("src1"))
    instanceRegistry.register(createDataObject("tgt1"))
    instanceRegistry.register(CustomDataFrameAction("action1", List(DataObjectId("src1")), List(DataObjectId("tgt1"))))
  }

  def testOptionsAndViewNameTokenAreReplacedAndSqlCanBeParsed(createDataObject: String => DataObject): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._
    registerAction(createDataObject)

    val emptyDf = Seq((1, "a")).toDF("num", "str")
    val customTransformer = SQLDfsTransformer(code = Map("tgt1" -> s"select %{inputViewName_src1}.num, %{option1} from %{inputViewName_src1}"))
    customTransformer.transformWithOptions(ActionId("action1"), Seq(), Map("src1" -> emptyDf), Map("option1" -> "str"))
  }

  def testLegacyViewNameWithoutPostfixIsStillSupportedAndSqlCanBeParsed(createDataObject: String => DataObject): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._
    registerAction(createDataObject)

    val emptyDf = Seq((1, "a")).toDF("num", "str")
    val customTransformer = SQLDfsTransformer(code = Map("tgt1" -> s"select src1.num, %{option1} from src1"))
    customTransformer.transformWithOptions(ActionId("action1"), Seq(), Map("src1" -> emptyDf), Map("option1" -> "str"))
  }
}
