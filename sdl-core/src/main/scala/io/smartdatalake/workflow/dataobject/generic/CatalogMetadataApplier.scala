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

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.{GenericSchema, GenericSchemaUtil}
import io.smartdatalake.workflow.dataobject.DataObject

/**
 * The metadata changes to be applied to the catalog for one DataObject.
 */
case class CatalogMetadataChanges(dataObjectId: DataObjectId,
                                  tableComment: Option[String] = None,
                                  columnComments: Map[Seq[String], String] = Map(),
                                  primaryKey: Option[Seq[String]] = None) {
  def isEmpty: Boolean = tableComment.isEmpty && columnComments.isEmpty && primaryKey.isEmpty

  def describe: Seq[String] = {
    tableComment.map(c => s"set table comment to '$c'").toSeq ++
      columnComments.toSeq.sortBy(_._1.mkString(".")).map { case (path, c) =>
        s"set comment of column ${GenericSchemaUtil.formatColumnPath(path)} to '$c'"
      } ++
      primaryKey.map(pk => s"create or replace primary key (${pk.mkString(", ")})").toSeq
  }
}

/**
 * Computes and applies the table metadata of a DataObject to the catalog.
 *
 * The desired state is taken from the SDLB configuration (table comment, primary key) and from the
 * exported schema files plus the Markdown description files (column comments). The current state is read
 * from the catalog, so that nothing is written when it is already up to date.
 *
 * This runs at deployment time and not during an SDLB run: table metadata can only change when the
 * configuration or the code changes, and writing it on every run causes unnecessary catalog load and
 * races with concurrent write operations.
 */
class CatalogMetadataApplier(schemaReader: DataObjectId => Option[GenericSchema],
                             columnDescriptions: Map[DataObjectId, Map[Seq[String], String]] = Map())
  extends SmartDataLakeLogger {

  /**
   * Compute the changes needed to bring the catalog in line with the configuration.
   * Returns None if the DataObject does not support catalog metadata at all.
   */
  def plan(dataObject: DataObject)(implicit context: ActionPipelineContext): Option[CatalogMetadataChanges] = {
    dataObject match {
      case tableDo: TableDataObject with CanHandleCatalogMetadata =>
        if (!tableDo.isTableExisting) {
          logger.warn(s"(${tableDo.id}) table ${tableDo.table.fullName} does not exist, skipping")
          None
        } else Some(planTable(tableDo))
      case _ =>
        logger.debug(s"(${dataObject.id}) does not support catalog metadata, skipping")
        None
    }
  }

  private def planTable(dataObject: TableDataObject with CanHandleCatalogMetadata)
                       (implicit context: ActionPipelineContext): CatalogMetadataChanges = {
    // table comment
    val desiredTableComment = dataObject.metadata.flatMap(_.description)
    val currentTableComment = dataObject.getTableComment
    val tableCommentChange = desiredTableComment.filterNot(currentTableComment.contains)

    // column comments: exported schema, overridden by the markdown description files
    val schemaComments = schemaReader(dataObject.id).map(GenericSchemaUtil.columnComments).getOrElse(Map())
    val describedComments = columnDescriptions.getOrElse(dataObject.id, Map())
    val desiredColumnComments = schemaComments ++ describedComments
    val currentColumnComments = dataObject.getColumnComments
    val columnCommentChanges = desiredColumnComments.filterNot { case (path, comment) =>
      currentColumnComments.get(path).contains(comment)
    }

    // primary key
    val primaryKeyChange = dataObject match {
      case constraintDo: CanHandleConstraints if dataObject.table.createAndReplacePrimaryKey =>
        val desired = dataObject.table.primaryKey
        val existing = constraintDo.getExistingPKConstraint(
          dataObject.table.catalog, dataObject.table.db, dataObject.table.name
        )
        def normalize(cols: Seq[String]) = cols.map(_.toLowerCase).toSet
        desired.filterNot(pk => existing.exists(e => normalize(e.pkColumns) == normalize(pk)))
      case _ => None
    }

    CatalogMetadataChanges(dataObject.id, tableCommentChange, columnCommentChanges, primaryKeyChange)
  }

  /**
   * Apply the given changes to the catalog.
   */
  def apply(dataObject: DataObject, changes: CatalogMetadataChanges)(implicit context: ActionPipelineContext): Unit = {
    dataObject match {
      case tableDo: TableDataObject with CanHandleCatalogMetadata =>
        changes.tableComment.foreach(tableDo.setTableComment)
        if (changes.columnComments.nonEmpty) tableDo.setColumnComments(changes.columnComments)
        changes.primaryKey.foreach { _ =>
          tableDo match {
            case constraintDo: CanHandleConstraints => constraintDo.createOrReplacePrimaryKeyConstraint
            case _ => logger.warn(s"(${dataObject.id}) primary key cannot be applied, DataObject does not support constraints")
          }
        }
      case _ => throw new IllegalStateException(s"(${dataObject.id}) does not support catalog metadata")
    }
  }
}
