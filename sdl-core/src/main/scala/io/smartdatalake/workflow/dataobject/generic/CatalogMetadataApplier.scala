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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe._
import io.smartdatalake.workflow.dataobject.DataObject

/**
 * The changes to be applied to the catalog for one DataObject.
 *
 * They are applied in two phases: everything belonging to the table itself in the first phase, and the
 * foreign keys in the second phase, as they can only be created once all referenced tables exist,
 * see [[CanHandleForeignKeys]].
 */
case class CatalogMetadataChanges(dataObjectId: DataObjectId,
                                  createTable: Option[GenericSchema] = None,
                                  schemaChanges: Seq[TableSchemaChange] = Seq(),
                                  tableComment: Option[String] = None,
                                  columnComments: Map[Seq[String], String] = Map(),
                                  primaryKey: Option[Seq[String]] = None,
                                  foreignKeys: Seq[ForeignKeyDefinition] = Seq()) {

  def isEmpty: Boolean = !hasTableChanges && !hasForeignKeyChanges

  /**
   * True if there is something to apply in the first phase.
   */
  def hasTableChanges: Boolean =
    createTable.isDefined || schemaChanges.nonEmpty || tableComment.isDefined || columnComments.nonEmpty || primaryKey.isDefined

  /**
   * True if there is something to apply in the second phase.
   */
  def hasForeignKeyChanges: Boolean = foreignKeys.nonEmpty

  def describe: Seq[String] = describeTableChanges ++ describeForeignKeys

  def describeTableChanges: Seq[String] = {
    createTable.map(schema => s"create table with columns ${schema.columns.mkString(", ")}").toSeq ++
      schemaChanges.map(_.describe) ++
      tableComment.map(c => s"set table comment to '$c'") ++
      columnComments.toSeq.sortBy(_._1.mkString(".")).map { case (path, c) =>
        s"set comment of column ${GenericSchemaUtil.formatColumnPath(path)} to '$c'"
      } ++
      primaryKey.map(pk => s"create or replace primary key (${pk.mkString(", ")})")
  }

  def describeForeignKeys: Seq[String] = foreignKeys.map(fk => s"create or replace ${fk.describe}")
}

/**
 * Computes and applies the table metadata of a DataObject to the catalog: the table itself, its schema,
 * the table and column comments, and the primary and foreign key constraints.
 *
 * The desired state is taken from the SDLB configuration (table comment, primary and foreign keys) and from the
 * exported schema files plus the Markdown description files (schema and column comments). The current state is
 * read from the catalog, so that nothing is written when it is already up to date.
 *
 * This runs at deployment time and not during an SDLB run: the schema and the metadata of a table can only
 * change when the configuration or the code changes, and writing it on every run causes unnecessary catalog
 * load and races with concurrent write operations.
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
      case tableDo: TableDataObject if isSupported(tableDo) =>
        val exportedSchema = schemaReader(tableDo.id)
        if (tableDo.isTableExisting) Some(planTable(tableDo, exportedSchema, createTable = None))
        else planCreateTable(tableDo, exportedSchema).map(schema => planTable(tableDo, exportedSchema, Some(schema)))
      case _ =>
        logger.debug(s"(${dataObject.id}) does not support catalog metadata, skipping")
        None
    }
  }

  private def isSupported(dataObject: TableDataObject): Boolean = {
    dataObject.isInstanceOf[CanHandleCatalogMetadata] || dataObject.isInstanceOf[CanHandleTableSchema] ||
      dataObject.isInstanceOf[CanHandleConstraints] || dataObject.isInstanceOf[CanHandleForeignKeys]
  }

  /**
   * The schema to create a missing table with, or None if it can not be created.
   */
  private def planCreateTable(dataObject: TableDataObject, exportedSchema: Option[GenericSchema])
                             (implicit context: ActionPipelineContext): Option[GenericSchema] = {
    val logPrefix = s"(${dataObject.id}) table ${dataObject.table.fullName} does not exist"
    dataObject match {
      case _: CanHandleTableSchema =>
        if (exportedSchema.isEmpty) logger.warn(s"$logPrefix and can not be created, no exported schema found." +
          " Note that schemas are exported for the output DataObjects of a run with '--test dry-run-with-schema-export'.")
        exportedSchema
      case _ =>
        logger.warn(s"$logPrefix and can not be created by ${dataObject.getClass.getSimpleName}, skipping")
        None
    }
  }

  private def planTable(dataObject: TableDataObject, exportedSchema: Option[GenericSchema], createTable: Option[GenericSchema])
                       (implicit context: ActionPipelineContext): CatalogMetadataChanges = {
    val tableExists = createTable.isEmpty

    // schema: the table is created with the exported schema, so afterwards only the not null columns of the
    // primary key are left to change.
    val schemaChanges = dataObject match {
      case schemaDo: CanHandleTableSchema =>
        val currentSchema = if (tableExists) schemaDo.getCurrentSchema else createTable
        (currentSchema, exportedSchema) match {
          case (Some(current), Some(desired)) => planSchemaChanges(dataObject.table, current, desired)
          case _ =>
            if (exportedSchema.isEmpty) logger.debug(s"(${dataObject.id}) no exported schema found, schema is not evolved")
            Seq()
        }
      case _ => Seq()
    }

    // table comment
    val tableCommentChange = dataObject match {
      case metadataDo: CanHandleCatalogMetadata =>
        val currentTableComment = if (tableExists) metadataDo.getTableComment else None
        dataObject.metadata.flatMap(_.description).filterNot(currentTableComment.contains)
      case _ => None
    }

    // column comments: exported schema, overridden by the markdown description files
    val columnCommentChanges = dataObject match {
      case metadataDo: CanHandleCatalogMetadata =>
        val schemaComments = exportedSchema.map(GenericSchemaUtil.columnComments).getOrElse(Map())
        val describedComments = columnDescriptions.getOrElse(dataObject.id, Map())
        val desiredColumnComments = schemaComments ++ describedComments
        // the catalog might return the column names in a different case than the exported schema, e.g. many
        // databases normalize unquoted identifiers to upper- or lowercase.
        val currentColumnComments = (if (tableExists) metadataDo.getColumnComments else Map[Seq[String], String]())
          .map { case (path, comment) => normalize(path) -> comment }
        desiredColumnComments.filterNot { case (path, comment) => currentColumnComments.get(normalize(path)).contains(comment) }
      case _ => Map[Seq[String], String]()
    }

    // primary key
    val primaryKeyChange = dataObject match {
      case constraintDo: CanHandleConstraints if dataObject.table.createAndReplacePrimaryKey =>
        val desired = dataObject.table.primaryKey
        val existing = if (tableExists) constraintDo.getExistingPKConstraint(
          dataObject.table.catalog, dataObject.table.db, dataObject.table.name
        ) else None
        desired.filterNot(pk => existing.exists(e => normalizeSet(e.pkColumns) == normalizeSet(pk)))
      case _ => None
    }

    // foreign keys
    val foreignKeyChanges = dataObject match {
      case foreignKeyDo: CanHandleForeignKeys if dataObject.table.createAndReplaceForeignKeys =>
        val existing = if (tableExists) foreignKeyDo.getExistingForeignKeys else Seq()
        foreignKeyDo.getDefinedForeignKeys.filterNot(fk => existing.exists(_.isEqualTo(fk)))
      case _ => Seq()
    }

    CatalogMetadataChanges(dataObject.id, createTable, schemaChanges, tableCommentChange, columnCommentChanges,
      primaryKeyChange, foreignKeyChanges)
  }

  // primary key columns are compared case-insensitively, as the catalog returns them in its own case
  private def normalizeSet(cols: Seq[String]): Set[String] = cols.map(_.toLowerCase).toSet

  private def normalize(identifiers: Seq[String]): Seq[String] =
    if (Environment.caseSensitive) identifiers else identifiers.map(_.toLowerCase)

  /**
   * Compare the schema registered in the catalog with the schema exported by the dry-run.
   *
   * New columns are added, changed data types are altered, and columns which are not written anymore are made
   * nullable, so that existing data is kept - this is the same behaviour as the schema evolution of an SDLB run,
   * see [[CanEvolveSchema]]. Columns are never dropped.
   * Primary key columns are made not null if [[Table.createAndReplacePrimaryKey]] is set.
   */
  private[smartdatalake] def planSchemaChanges(table: Table, current: GenericSchema, desired: GenericSchema): Seq[TableSchemaChange] = {
    val notNullColumns = if (table.createAndReplacePrimaryKey) table.primaryKey.toSeq.flatten else Seq()
    diffFields(current.fields, desired.fields, Seq()) ++ planNotNullChanges(current.fields, desired.fields, notNullColumns)
  }

  private def diffFields(currentFields: Seq[GenericField], desiredFields: Seq[GenericField], parents: Seq[String]): Seq[TableSchemaChange] = {
    val changedFields = desiredFields.flatMap { desiredField =>
      val path = parents :+ desiredField.name
      findField(currentFields, desiredField.name) match {
        case None => Seq(AddColumn(path, desiredField.dataType, desiredField.comment))
        case Some(currentField) => (currentField.dataType, desiredField.dataType) match {
          case (currentStruct: GenericStructDataType, desiredStruct: GenericStructDataType) =>
            diffFields(currentStruct.fields, desiredStruct.fields, path)
          case (currentType, desiredType) if !currentType.isSameType(desiredType) =>
            Seq(ChangeColumnType(path, desiredType, currentType))
          case _ => Seq()
        }
      }
    }
    // columns which are not written anymore must be nullable to be able to write new records without them
    val removedFields = currentFields
      .filter(currentField => findField(desiredFields, currentField.name).isEmpty)
      .filterNot(_.nullable)
      .map(currentField => ChangeColumnNullable(parents :+ currentField.name, nullable = true))
    changedFields ++ removedFields
  }

  /**
   * Primary key columns must be not null. Note that this fails if the table already contains null values,
   * which is the correct behaviour as the primary key can not be created in that case either.
   */
  private def planNotNullChanges(currentFields: Seq[GenericField], desiredFields: Seq[GenericField], notNullColumns: Seq[String]): Seq[TableSchemaChange] = {
    notNullColumns.flatMap { column =>
      // a column to be added is added as nullable, as existing records have no value for it
      val isNullable = findField(currentFields, column).map(_.nullable)
        .getOrElse(findField(desiredFields, column).exists(_.nullable))
      if (isNullable) Some(ChangeColumnNullable(Seq(column), nullable = false)) else None
    }
  }

  private def findField(fields: Seq[GenericField], name: String): Option[GenericField] = {
    if (Environment.caseSensitive) fields.find(_.name == name)
    else fields.find(_.name.equalsIgnoreCase(name))
  }

  /**
   * Apply the changes of the first phase to the catalog: create the table, evolve its schema, and write the
   * comments and the primary key.
   */
  def applyTableChanges(dataObject: DataObject, changes: CatalogMetadataChanges)(implicit context: ActionPipelineContext): Unit = {
    val tableDo = dataObject match {
      case tableDo: TableDataObject => tableDo
      case _ => throw new IllegalStateException(s"(${dataObject.id}) does not support catalog metadata")
    }
    (tableDo, changes.createTable) match {
      case (schemaDo: CanHandleTableSchema, Some(schema)) => schemaDo.createTable(schema)
      case (_, Some(_)) => logger.warn(s"(${dataObject.id}) table can not be created, DataObject does not support schema changes")
      case _ => ()
    }
    tableDo match {
      case schemaDo: CanHandleTableSchema if changes.schemaChanges.nonEmpty => schemaDo.applySchemaChanges(changes.schemaChanges)
      case _ if changes.schemaChanges.nonEmpty => logger.warn(s"(${dataObject.id}) schema changes can not be applied, DataObject does not support schema changes")
      case _ => ()
    }
    tableDo match {
      case metadataDo: CanHandleCatalogMetadata =>
        changes.tableComment.foreach(metadataDo.setTableComment)
        if (changes.columnComments.nonEmpty) metadataDo.setColumnComments(changes.columnComments)
      case _ => ()
    }
    changes.primaryKey.foreach { _ =>
      tableDo match {
        case constraintDo: CanHandleConstraints => constraintDo.createOrReplacePrimaryKeyConstraint
        case _ => logger.warn(s"(${dataObject.id}) primary key can not be applied, DataObject does not support constraints")
      }
    }
  }

  /**
   * Apply the changes of the second phase to the catalog: create the foreign keys.
   * This must be done after [[applyTableChanges]] of *all* DataObjects, as a foreign key can only be created
   * once the referenced table exists including its primary key.
   */
  def applyForeignKeys(dataObject: DataObject, changes: CatalogMetadataChanges)(implicit context: ActionPipelineContext): Unit = {
    if (changes.foreignKeys.nonEmpty) {
      dataObject match {
        case foreignKeyDo: CanHandleForeignKeys => foreignKeyDo.createOrReplaceForeignKeyConstraints(changes.foreignKeys)
        case _ => logger.warn(s"(${dataObject.id}) foreign keys can not be applied, DataObject does not support foreign keys")
      }
    }
  }

  /**
   * Apply all changes of a DataObject to the catalog.
   * Note that DataObjectSchemaExporter applies the two phases separately for all DataObjects,
   * see [[applyTableChanges]] and [[applyForeignKeys]].
   */
  def apply(dataObject: DataObject, changes: CatalogMetadataChanges)(implicit context: ActionPipelineContext): Unit = {
    applyTableChanges(dataObject, changes)
    applyForeignKeys(dataObject, changes)
  }
}
