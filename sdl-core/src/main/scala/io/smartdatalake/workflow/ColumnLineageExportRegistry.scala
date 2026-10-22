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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.ColumnLineage

import scala.collection.mutable

/**
 * Collects the column level lineage of the DataFrames written to output DataObjects during the init phase,
 * so it can be exported at the end of a dry-run, see [[io.smartdatalake.app.TestMode.DryRunWithLineageExport]].
 *
 * The lineage is taken from the init phase DataFrames, as these are the DataFrames SDLB assembles from the
 * transformations configured for an Action. In the exec phase the same DataFrames are used, so the lineage is
 * the same, but a dry-run does not need access to the data.
 */
private[smartdatalake] class ColumnLineageExportRegistry extends SmartDataLakeLogger {

  private val lineages = mutable.Map[DataObjectId, ColumnLineageEntry]()

  /**
   * Init phase: remember the column lineage of the DataFrame written to `dataObjectId` by `actionId`.
   * If a DataObject is written more than once, the last lineage wins.
   */
  def register(actionId: ActionId, dataObjectId: DataObjectId, lineage: ColumnLineage): Unit = synchronized {
    logger.debug(s"($actionId) registering column lineage of $dataObjectId for export")
    lineages.update(dataObjectId, ColumnLineageEntry(actionId, lineage))
  }

  /**
   * All column lineages collected so far.
   */
  def getColumnLineages: Map[DataObjectId, ColumnLineageEntry] = synchronized {
    lineages.toMap
  }

  def isEmpty: Boolean = synchronized(lineages.isEmpty)
}

/**
 * The column lineage of an output DataObject, together with the Action which created it.
 */
private[smartdatalake] case class ColumnLineageEntry(actionId: ActionId, lineage: ColumnLineage)
