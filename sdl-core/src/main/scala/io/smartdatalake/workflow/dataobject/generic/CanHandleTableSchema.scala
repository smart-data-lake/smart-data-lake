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
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.{GenericDataType, GenericSchema, GenericSchemaUtil}

/**
 * A single change of the schema of a table, as computed by [[CatalogMetadataApplier]] by comparing the
 * schema exported by a dry-run with the schema currently registered in the catalog.
 *
 * Columns are addressed by their path to support nested columns, see [[GenericSchemaUtil.columnComments]].
 */
sealed trait TableSchemaChange {
  def columnPath: Seq[String]

  def columnName: String = GenericSchemaUtil.formatColumnPath(columnPath)

  def describe: String
}

/**
 * Add a column which exists in the new schema but not in the catalog.
 * Note that new columns must be nullable, as existing records have no value for them.
 */
case class AddColumn(columnPath: Seq[String], dataType: GenericDataType, comment: Option[String] = None) extends TableSchemaChange {
  override def describe: String = s"add column $columnName ${dataType.sql}"
}

/**
 * Change the data type of an existing column.
 */
case class ChangeColumnType(columnPath: Seq[String], dataType: GenericDataType, currentDataType: GenericDataType) extends TableSchemaChange {
  override def describe: String = s"change data type of column $columnName from ${currentDataType.sql} to ${dataType.sql}"
}

/**
 * Change the nullability of an existing column.
 * Columns which are removed from the new schema are made nullable, so that existing data is kept and
 * new records can be written without them. Primary key columns are made not null,
 * see [[Table.createAndReplacePrimaryKey]].
 */
case class ChangeColumnNullable(columnPath: Seq[String], nullable: Boolean) extends TableSchemaChange {
  override def describe: String = s"make column $columnName ${if (nullable) "nullable" else "not null"}"
}

/**
 * This trait defines how the schema of a TableDataObject is created and evolved in the catalog.
 *
 * Like the table and column comments of [[CanHandleCatalogMetadata]], this is *not* applied during a normal
 * SDLB run, but at deployment time by DataObjectSchemaExporter. The desired schema is the one exported by a
 * dry-run, see [[io.smartdatalake.app.TestMode.DryRunWithSchemaExport]]. This allows to create and migrate the
 * tables of an environment before the data pipeline runs there, and to review the changes with mode "plan".
 *
 * Implementations must address the table by its fully qualified name (see [[Table.fullName]]) and must not
 * change the current catalog or schema of the session, see [[CanHandleCatalogMetadata]].
 */
trait CanHandleTableSchema { self: TableDataObject with CanWriteDataFrame =>

  /**
   * The schema currently registered in the catalog, or None if the table does not exist yet.
   *
   * The default implementation reads the schema of the table, which for table DataObjects is the schema
   * as registered in the catalog. Override if the DataObject can get more precise information, e.g. about
   * the nullability of the columns.
   */
  def getCurrentSchema(implicit context: ActionPipelineContext): Option[GenericSchema] = {
    if (isTableExisting) Some(getDataFrame().schema) else None
  }

  /**
   * Create the table with the given schema.
   *
   * The default implementation writes an empty DataFrame, so that the table is created the same way as it
   * would be created by the first run of the data pipeline, including its location, partitioning and options.
   */
  def createTable(schema: GenericSchema)(implicit context: ActionPipelineContext): Unit = {
    val emptyDf = schema.convert(writeSubFeedSupportedTypes.head).getEmptyDataFrame(id)
    // writing no records is exactly what we want here, so the "no data" checks must not stop us
    val enableSparkPlanNoDataCheckOrig = Environment._enableSparkPlanNoDataCheck
    Environment._enableSparkPlanNoDataCheck = Some(false)
    try {
      writeDataFrame(emptyDf, Seq())
    } catch {
      case _: NoDataToProcessWarning => logger.debug(s"($id) no data written when creating table ${table.fullName}")
    } finally {
      Environment._enableSparkPlanNoDataCheck = enableSparkPlanNoDataCheckOrig
    }
    require(isTableExisting, s"($id) table ${table.fullName} doesn't exist even though we tried to create it")
  }

  /**
   * Apply the given schema changes to the catalog.
   */
  def applySchemaChanges(changes: Seq[TableSchemaChange])(implicit context: ActionPipelineContext): Unit
}
