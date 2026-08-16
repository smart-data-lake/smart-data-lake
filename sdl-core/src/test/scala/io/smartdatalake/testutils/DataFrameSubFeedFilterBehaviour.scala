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
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, ColumnFilter, DataFrameSubFeed}

import scala.reflect.runtime.universe.Type

/**
 * Behaviour tests for the column-bound filters of a [[DataFrameSubFeed]], engine-agnostic so they can be
 * instantiated against any [[io.smartdatalake.workflow.dataframe.GenericDataFrame]] implementation.
 */
trait DataFrameSubFeedFilterBehaviour {

  def subFeedType: Type
  implicit def instanceRegistry: InstanceRegistry
  implicit def context: ActionPipelineContext

  private lazy val helper = DataFrameSubFeed.getCompanion(subFeedType)

  /**
   * A SubFeed on a DataFrame with columns "id" and "value", holding 3 rows with id 1..3.
   */
  private def testSubFeed: DataFrameSubFeed = {
    import helper.implicits._
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    helper.getSubFeed(df, DataObjectId("dataObjectId"), Seq())
  }

  private def ids(subFeed: DataFrameSubFeed): Seq[Int] =
    subFeed.dataFrame.get.collect.map(_.getAs[Int](0)).toSeq.sorted

  def testApplyFilterOnExistingColumn(): Unit = {
    val subFeed = testSubFeed.withFilters(Seq(ColumnFilter("id", "id > 1"))).applyFilter
    assert(ids(subFeed) == Seq(2, 3))
  }

  def testSkipFilterOnMissingColumn(): Unit = {
    // the filter must be silently skipped, otherwise the expression could not be resolved
    val subFeed = testSubFeed.withFilters(Seq(ColumnFilter("notExisting", "notExisting > 1"))).applyFilter
    assert(ids(subFeed) == Seq(1, 2, 3))
  }

  def testApplyMultipleFiltersConjunctively(): Unit = {
    val filters = Seq(ColumnFilter("id", "id > 1"), ColumnFilter("value", "value = 'b'"))
    val subFeed = testSubFeed.withFilters(filters).applyFilter
    assert(ids(subFeed) == Seq(2))
  }

  def testApplyFilterCombinedWithPartitionValues(): Unit = {
    val subFeed = testSubFeed.withFilters(Seq(PartitionValues(Map("value" -> "b")), PartitionValues(Map("value" -> "c"))),
      Seq(ColumnFilter("id", "id > 2")))
    assert(ids(subFeed) == Seq(3))
  }

  def testUpdateFiltersDropsMissingColumns(): Unit = {
    val filters = Seq(ColumnFilter("id", "id > 1"), ColumnFilter("notExisting", "notExisting > 1"))
    val subFeed = testSubFeed.withFilters(filters)
    val updated = subFeed.updateFilters(subFeed.schema)
    assert(updated.filters == Seq(ColumnFilter("id", "id > 1")))
  }

  def testUpdateFiltersCaseInsensitiveByDefault(): Unit = {
    val previousCaseSensitive = Environment._caseSensitive
    Environment._caseSensitive = Some(false)
    try {
      val subFeed = testSubFeed.withFilters(Seq(ColumnFilter("ID", "ID > 1")))
      assert(subFeed.updateFilters(subFeed.schema).filters.size == 1)
    } finally {
      Environment._caseSensitive = previousCaseSensitive
    }
  }

  def testClearFilters(): Unit = {
    val subFeed = testSubFeed.withFilters(Seq(ColumnFilter("id", "id > 1")))
    assert(subFeed.hasFilters)
    val cleared = subFeed.clearFilters()
    assert(cleared.filters.isEmpty)
    // clearing filters also drops the DataFrame, so that a fresh unfiltered one is read
    assert(cleared.dataFrame.isEmpty)
  }

  def testClearFiltersKeepsDataFrameIfNoFilters(): Unit = {
    val subFeed = testSubFeed
    assert(!subFeed.hasFilters)
    val cleared = subFeed.clearFilters()
    assert(cleared.filters.isEmpty)
    assert(cleared.dataFrame.isDefined)
  }

  def testAddFiltersReplacesSameColumn(): Unit = {
    val subFeed = testSubFeed.withFilters(Seq(ColumnFilter("id", "id > 1")))
    val added = subFeed.addFilters(Seq(ColumnFilter("id", "id > 2"), ColumnFilter("value", "value = 'c'")))
    assert(added.filters == Seq(ColumnFilter("id", "id > 2"), ColumnFilter("value", "value = 'c'")))
    assert(ids(added.applyFilter) == Seq(3))
  }

  def testUnionKeepsOnlyCommonFilters(): Unit = {
    val filterId = ColumnFilter("id", "id > 1")
    val filterValue = ColumnFilter("value", "value = 'b'")
    val subFeed1 = testSubFeed.withFilters(Seq(filterId, filterValue))
    val subFeed2 = testSubFeed.withFilters(Seq(filterId))
    val unioned = subFeed1.union(subFeed2).asInstanceOf[DataFrameSubFeed]
    // dropping a filter widens the data read, whereas keeping one not valid for the other SubFeed would lose data
    assert(unioned.filters == Seq(filterId))
  }
}
