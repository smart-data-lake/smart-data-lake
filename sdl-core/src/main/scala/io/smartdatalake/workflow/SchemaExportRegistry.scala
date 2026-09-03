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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.dataframe.GenericSchema

import scala.collection.mutable

/**
 * Collects the schemas written to output DataObjects during the init phase, so they can be exported
 * at the end of a dry-run, see [[io.smartdatalake.app.TestMode.DryRunWithSchemaExport]].
 *
 * The schemas are taken from the init phase DataFrames and therefore include the column comments
 * assembled by SDLB, e.g. from schemaMin or from the ScalaDoc of case classes returned by user defined
 * functions. Asking the DataObject for its schema instead would read it back from the catalog, which
 * does not work at deployment time when the table might not exist yet.
 */
private[smartdatalake] class SchemaExportRegistry extends SmartDataLakeLogger {

  private val schemas = mutable.Map[DataObjectId, GenericSchema]()

  /**
   * Init phase: remember the schema written to `dataObjectId`.
   * If an Action writes the same DataObject more than once, the last schema wins.
   */
  def register(dataObjectId: DataObjectId, schema: GenericSchema): Unit = synchronized {
    logger.debug(s"($dataObjectId) registering schema for export")
    schemas.update(dataObjectId, schema)
  }

  /**
   * All schemas collected so far.
   */
  def getSchemas: Map[DataObjectId, GenericSchema] = synchronized {
    schemas.toMap
  }

  def isEmpty: Boolean = synchronized(schemas.isEmpty)
}
