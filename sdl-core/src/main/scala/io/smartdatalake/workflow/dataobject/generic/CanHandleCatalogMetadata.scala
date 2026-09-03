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
import io.smartdatalake.workflow.dataframe.GenericSchemaUtil

/**
 * This trait defines how table and column comments are read from and written to the catalog of a
 * TableDataObject.
 *
 * Note that comments are *not* applied during a normal SDLB run: table metadata can only change when the
 * configuration or the code changes, so it is applied at deployment time by DataObjectSchemaExporter,
 * which reads the desired state from the configuration and from the exported schema files.
 * See [[io.smartdatalake.app.TestMode.DryRunWithSchemaExport]] for how these schema files are created.
 *
 * Implementations must address the table by its fully qualified name (see [[Table.fullName]]) and must not
 * change the current catalog or schema of the session, otherwise concurrent statements of a parallel run
 * resolve table names in the wrong schema.
 */
trait CanHandleCatalogMetadata { self: TableDataObject =>

  /**
   * Read the table comment currently set in the catalog.
   */
  def getTableComment(implicit context: ActionPipelineContext): Option[String]

  /**
   * Write the table comment to the catalog.
   */
  def setTableComment(comment: String)(implicit context: ActionPipelineContext): Unit

  /**
   * Read the column comments currently set in the catalog, keyed by column path,
   * see [[GenericSchemaUtil.columnComments]].
   *
   * The default implementation reads the schema of the table, which for table DataObjects is the schema
   * as registered in the catalog, including its comments.
   */
  def getColumnComments(implicit context: ActionPipelineContext): Map[Seq[String], String] = {
    GenericSchemaUtil.columnComments(getDataFrame().schema)
  }

  /**
   * Write the given column comments to the catalog, keyed by column path.
   */
  def setColumnComments(comments: Map[Seq[String], String])(implicit context: ActionPipelineContext): Unit
}
