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
package io.smartdatalake.workflow.dataobject.generic

import io.smartdatalake.definitions.Environment
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, SchemaViolationException}
import org.apache.spark.sql.DataFrame

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

  /**
   * Validate the schema of a given Spark Data Frame `df` that it contains the specified primary key columns
   *
   * @param df   The data frame to validate.
   * @param role role used in exception message. Set to read or write.
   * @param obj  object used in exception message..
   * @throws SchemaViolationException if the partitions columns are not included.
   */
  def validateSchemaHasPrimaryKeyCols(df: DataFrame, role: String, obj: String = "DataFrame"): Unit = {
    table.primaryKey.foreach { pk =>
      val missingCols = if (Environment.caseSensitive) pk.diff(df.columns)
      else pk.map(_.toLowerCase).diff(df.columns.map(_.toLowerCase))
      if (missingCols.nonEmpty) throw new SchemaViolationException(s"($id) $obj is missing primary key cols ${missingCols.mkString(", ")} on $role")
    }
  }
}
