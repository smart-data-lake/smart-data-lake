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

import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.DataObject

/**
 * A DataObject which stores the query of the DataFrame written to it instead of its data, e.g. a database view.
 *
 * The query is then evaluated on every read, so it must not depend on the current run: an Action writing a
 * ViewDataObject ignores the partition values and filters of its input SubFeeds, as they would otherwise become part
 * of the stored query, and it must not have an execution mode.
 * Partition values of the main input are still passed on to the output SubFeed of a partitioned ViewDataObject,
 * so that the next Action reads the view filtered by them.
 */
trait ViewDataObject extends DataObject {

  /**
   * True if the view is materialized, i.e. the database stores the result of its query.
   */
  def isMaterialized: Boolean = false

  /**
   * The query of the view for a DataFrame written to it, in the SQL dialect of the database.
   * It is exported by a dry-run with schema export, so that CatalogSchemaUpdater can create or replace the view at
   * deployment time, see [[CatalogMetadataApplier]].
   */
  def getViewQuery(df: GenericDataFrame)(implicit context: ActionPipelineContext): String

  /**
   * The definition of the existing view as stored by the database, or None if the view does not exist.
   * Depending on the database this can also be a whole `CREATE VIEW` statement.
   */
  def getExistingViewDefinition(implicit context: ActionPipelineContext): Option[String]

  /**
   * True if the definition of the existing view has the given query. Databases reformat the query of a view, so
   * both are normalized before comparing. If they can not be compared, false is returned and the view is replaced.
   */
  def isSameViewQuery(existingDefinition: String, query: String)(implicit context: ActionPipelineContext): Boolean

  /**
   * True if the existing view has the given query, see [[isSameViewQuery]]. False if the view does not exist.
   */
  def isViewUpToDate(query: String)(implicit context: ActionPipelineContext): Boolean =
    getExistingViewDefinition.exists(isSameViewQuery(_, query))

  /**
   * Create or replace the view with the given query. A materialized view is populated with the result of the query.
   */
  def createOrReplaceView(query: String)(implicit context: ActionPipelineContext): Unit
}
