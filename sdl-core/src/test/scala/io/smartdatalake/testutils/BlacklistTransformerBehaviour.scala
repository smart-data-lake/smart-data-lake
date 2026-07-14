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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.DataFrameSubFeed
import io.smartdatalake.workflow.action.generic.transformer.BlacklistTransformer

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[BlacklistTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 */
trait BlacklistTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testOnlyMatchingColumnsRemoved(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val blacklistTransformer = BlacklistTransformer(columnBlacklist = Seq("column1", "column3"))
    val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

    val transformed = blacklistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column2"))
  }

  def testCaseInsensitiveByDefault(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val blacklistTransformer = BlacklistTransformer(columnBlacklist = Seq("ColumN1"))
    val df = Seq(Tuple1(1), Tuple1(2)).toDF("column1")

    val transformed = blacklistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns.isEmpty)
  }

  def testCaseSensitiveIfEnvironmentCaseSensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(true)
    try {
      val blacklistTransformer = BlacklistTransformer(columnBlacklist = Seq("ColumN1"))
      val df = Seq((1, 1), (2, 2)).toDF("column1", "ColumN1")

      val transformed = blacklistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

      assert(transformed.schema.columns == Seq("column1"))
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }

  def testNoErrorIfRemainingColumnHasDots(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val blacklistTransformer = BlacklistTransformer(columnBlacklist = Seq("column.2"))
    val df = Seq((1, 1), (2, 2)).toDF("column.1", "column.2")

    val transformed = blacklistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column.1"))
  }
}
