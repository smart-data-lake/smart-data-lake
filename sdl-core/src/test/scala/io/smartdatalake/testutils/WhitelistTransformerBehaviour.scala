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
import io.smartdatalake.workflow.action.generic.transformer.WhitelistTransformer

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for [[WhitelistTransformer]], engine-agnostic so they can be instantiated against any
 * [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation (Spark, plain-Scala, ...).
 */
trait WhitelistTransformerBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  def testOnlyMatchingColumnsWhitelisted(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("column1", "column3"))
    val df = Seq((1, 1), (2, 2)).toDF("column1", "column2")

    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column1"))
  }

  def testCaseInsensitiveByDefault(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("coLumn1"))
    val df = Seq(Tuple1(1), Tuple1(2)).toDF("column1")

    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column1"))
  }

  def testCaseSensitiveIfEnvironmentCaseSensitive(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(true)
    try {
      val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("ColumN1", "blop"))
      val df = Seq((1, 1), (2, 2)).toDF("column1", "ColumN1")

      val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

      assert(transformed.schema.columns == Seq("ColumN1"))
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }

  def testNoErrorIfWhitelistedColumnHasDots(): Unit = {
    val helper = DataFrameSubFeed.getCompanion(subFeedType)
    import helper.implicits._

    val whitelistTransformer = WhitelistTransformer(columnWhitelist = Seq("column.1"))
    val df = Seq(Tuple1(1), Tuple1(2)).toDF("column.1")

    val transformed = whitelistTransformer.transform("id", Seq(), df, DataObjectId("dataObjectId"), None, Map())

    assert(transformed.schema.columns == Seq("column.1"))
  }
}
