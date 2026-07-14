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
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.generic.transformer.StandardizeColNamesTransformer

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[StandardizeColNamesTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 */
trait StandardizeColNamesTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testDotsInColumnNamesAreRemoved(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val colNamesTransformer = StandardizeColNamesTransformer()
    val df = Seq((1, 1), (2, 2)).toDF("one.dot", "two.do.ts")

    val transformed = colNamesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("onedot", "twodots"))
  }

  def testBlanksInColumnNamesAreReplaced(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val colNamesTransformer = StandardizeColNamesTransformer(removeNonStandardSQLNameChars = false, replaceNonStandardSQLNameCharsWithUnderscores = true)
    val df = Seq((1, 1, 1), (2, 2, 2)).toDF("one dot", "two-do-ts", "value of property!in$$")

    val transformed = colNamesTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("one_dot", "two_do_ts", "value_of_property_in_"))
  }
}
