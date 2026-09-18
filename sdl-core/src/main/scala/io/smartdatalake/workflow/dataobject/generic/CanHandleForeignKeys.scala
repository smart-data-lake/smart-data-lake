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

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.DataObject

import scala.util.Try

/**
 * A foreign key constraint as defined in the configuration ([[Table.foreignKeys]]) or as read from the catalog.
 *
 * @param columns         mapping of the columns of this table to the columns of the referenced table
 * @param referencedTable fully qualified name of the referenced table
 * @param name            constraint name. It can be empty for an existing constraint,
 *                        since some databases have constraints without names.
 */
case class ForeignKeyDefinition(columns: Map[String, String], referencedTable: String, name: Option[String] = None) {

  /**
   * Compare two foreign keys by their content, ignoring the constraint name, the case of the identifiers
   * and the quoting of the referenced table name.
   */
  def isEqualTo(other: ForeignKeyDefinition): Boolean =
    normalizedColumns == other.normalizedColumns && normalizedTable == other.normalizedTable

  def describe: String =
    s"foreign key ${name.map(_ + " ").getOrElse("")}(${columns.keys.mkString(", ")})" +
      s" references $referencedTable (${columns.values.mkString(", ")})"

  private def normalizedColumns: Map[String, String] = columns.map { case (k, v) => (normalize(k), normalize(v)) }

  private def normalizedTable: String = normalize(referencedTable)

  private def normalize(identifier: String) = identifier.toLowerCase.replace("\"", "")
}

/**
 * This trait defines how foreign key constraints of a TransactionalTableDataObject are read from and written
 * to the catalog.
 *
 * Note that foreign keys are not applied during a normal SDLB run. They are applied at deployment time by
 * CatalogSchemaUpdater, see [[CanHandleCatalogMetadata]], and only if [[Table.createAndReplaceForeignKeys]]
 * is set to true. This needs two phases: all tables must be created including their primary keys, before the
 * foreign keys referencing them can be applied.
 */
trait CanHandleForeignKeys { self: TransactionalTableDataObject =>

  /**
   * Read the foreign key constraints currently defined in the catalog for this table.
   */
  def getExistingForeignKeys(implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition]

  def dropForeignKeyConstraint(constraintName: String)(implicit context: ActionPipelineContext): Unit

  def createForeignKeyConstraint(foreignKey: ForeignKeyDefinition)(implicit context: ActionPipelineContext): Unit

  /**
   * The foreign keys defined in the configuration of this DataObject, with the referenced DataObjects
   * resolved to their tables.
   */
  def getDefinedForeignKeys(implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition] = {
    table.foreignKeys.toSeq.flatten.map { fk =>
      val referencedTable = getReferencedTable(fk)
      ForeignKeyDefinition(fk.columns, referencedTable.fullName, Some(foreignKeyConstraintName(fk, referencedTable)))
    }
  }

  /**
   * The table referenced by a foreign key, looked up from the DataObject it refers to.
   *
   * Catalog and database default to the ones of this table, e.g. if the referenced DataObject gets them
   * from the session and not from its configuration or connection.
   */
  def getReferencedTable(fk: ForeignKey)(implicit context: ActionPipelineContext): Table = {
    val describeForeignKey = s"foreign key ${fk.name.map(_ + " ").getOrElse("")}(${fk.columns.keys.mkString(", ")})"
    val referencedDataObject = Try(context.instanceRegistry.get[DataObject](fk.dataObjectId))
      .getOrElse(throw ConfigurationException(s"($id) $describeForeignKey references" +
        s" ${fk.dataObjectId}, which is not defined as a DataObject in the configuration"))
    referencedDataObject match {
      case tableDataObject: TableDataObject =>
        tableDataObject.table.overrideCatalogAndDb(table.catalog, table.db)
      case _ => throw ConfigurationException(s"($id) $describeForeignKey references DataObject ${fk.dataObjectId}" +
        s" of type ${referencedDataObject.getClass.getSimpleName}, which has no table")
    }
  }

  /**
   * Constraint name to use, defaults to sdlb_"tableName"_"referencedTableName"_fk.
   */
  def foreignKeyConstraintName(fk: ForeignKey, referencedTable: Table): String =
    fk.name.getOrElse(s"sdlb_${table.name}_${referencedTable.name}_fk")

  /**
   * Create the given foreign key constraints, replacing an existing constraint with the same name or content.
   */
  def createOrReplaceForeignKeyConstraints(foreignKeys: Seq[ForeignKeyDefinition])(implicit context: ActionPipelineContext): Unit = {
    val existingForeignKeys = getExistingForeignKeys
    foreignKeys.foreach { foreignKey =>
      existingForeignKeys.find(e => matchesByName(e, foreignKey) || e.isEqualTo(foreignKey)).foreach { existing =>
        existing.name match {
          case Some(name) => dropForeignKeyConstraint(name)
          case None => logger.warn(s"($id) can not replace foreign key ${existing.describe} as the constraint name" +
            s" returned by the catalog is empty")
        }
      }
      createForeignKeyConstraint(foreignKey)
    }
  }

  private def matchesByName(a: ForeignKeyDefinition, b: ForeignKeyDefinition) =
    a.name.exists(nameA => b.name.exists(_.equalsIgnoreCase(nameA)))
}
