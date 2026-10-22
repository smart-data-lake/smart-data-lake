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
package io.smartdatalake.workflow.dataframe.spark

import io.smartdatalake.config.InstanceRegistry
import io.smartdatalake.testutils.DataFrameSubFeedFilterBehaviour
import io.smartdatalake.testutils.spark.SparkTestUtil
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.runtime.universe.{Type, typeOf}

class SparkSubFeedFilterTest extends AnyFunSuite with DataFrameSubFeedFilterBehaviour {

  protected implicit val session: SparkSession = SparkTestUtil.session

  override def subFeedType: Type = typeOf[SparkSubFeed]
  implicit val instanceRegistry: InstanceRegistry = new InstanceRegistry()
  implicit val context: ActionPipelineContext = SparkTestUtil.getDefaultActionPipelineContext

  test("a filter is applied if its column exists") {
    testApplyFilterOnExistingColumn()
  }

  test("a filter is skipped if its column does not exist") {
    testSkipFilterOnMissingColumn()
  }

  test("multiple filters are applied conjunctively") {
    testApplyMultipleFiltersConjunctively()
  }

  test("filters are applied together with partition values") {
    testApplyFilterCombinedWithPartitionValues()
  }

  test("updateFilters drops filters for non-existing columns") {
    testUpdateFiltersDropsMissingColumns()
  }

  test("updateFilters matches the column case insensitive per default") {
    testUpdateFiltersCaseInsensitiveByDefault()
  }

  test("clearFilters empties the filters and breaks the lineage") {
    testClearFilters()
  }

  test("clearFilters keeps the DataFrame if there were no filters") {
    testClearFiltersKeepsDataFrameIfNoFilters()
  }

  test("addFilters replaces an existing filter on the same column") {
    testAddFiltersReplacesSameColumn()
  }

  test("union keeps only the filters present in both SubFeeds") {
    testUnionKeepsOnlyCommonFilters()
  }
}
