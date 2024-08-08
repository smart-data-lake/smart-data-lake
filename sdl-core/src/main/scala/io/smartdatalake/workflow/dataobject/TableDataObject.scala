/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

import scala.reflect.runtime.universe.Type

trait TableDataObject extends DataObject with CanCreateDataFrame with SchemaValidation {

  var table: Table

  def isDbExisting(implicit context: ActionPipelineContext): Boolean

  def isTableExisting(implicit context: ActionPipelineContext): Boolean

  def dropTable(implicit context: ActionPipelineContext): Unit

  def getPKduplicates(subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._
    if (table.primaryKey.isEmpty) {
      getDataFrame(Seq(), subFeedType).filter(lit(false)) // get empty dataframe
    } else {
      getDataFrame(Seq(), subFeedType).getNonuniqueRows(table.primaryKey.get)
    }
  }

  def getPKnulls(subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(subFeedType)
    import functions._
    if (table.primaryKey.isEmpty) {
      getDataFrame(Seq(), subFeedType).filter(lit(false)) // get empty dataframe
    } else {
      getDataFrame(Seq(), subFeedType).getNulls(table.primaryKey.get)
    }
  }

  def getPKviolators(subFeedType: Type)(implicit context: ActionPipelineContext): GenericDataFrame = {
    getPKduplicates(subFeedType).unionByName(getPKnulls(subFeedType))
  }

  override def atlasQualifiedName(prefix: String): String = s"${table.db.getOrElse("default")}.${table.name}"

  override def atlasName: String = id.id

  /**
   * Returns statistics about this DataObject from the catalog. Depending on it's type this can be
   * - min
   * - max
   * - num_nulls -> Completness %
   * - distinct_count -> Uniqness %
   * - avg_col_len	11
   * - max_col_len	13
   * - ...
   * @param update if true, more costly operations such as "analyze table ... compute statistics for all columns" are executed before returning results.*
   * @param lastModifiedAt can be given to avoid update if there has been no new data written to the table.
   * @return column statistics about this DataObject
   */
  def getColumnStats(update: Boolean = false, lastModifiedAt: Option[Long] = None)(implicit context: ActionPipelineContext): Map[String, Map[String, Any]] = Map()
}
