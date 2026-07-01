/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2024 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.jdbc.JdbcCatalog

/**
 * Concrete implementation of [[CanHandleReferentialKeys]] for JDBC-backed DataObjects.
 *
 * All seven abstract methods are implemented here; subclasses only need to provide four
 * protected hooks that point at the connection's catalog and metadata access:
 *
 * {{{
 *   override protected def jCatalog: JdbcCatalog = connection.catalog
 *   override protected def fetchPrimaryKey(c, s, t) = connection.getJdbcPrimaryKey(c, s, t)
 *   override protected def fetchForeignKeys(c, s, t) = connection.getJdbcForeignKeys(c, s, t)
 *   override protected def fetchColumnNullability(c, s, t) = connection.getColumnNullability(c, s, t)
 * }}}
 *
 * Used by [[JdbcTableDataObject]] and [[SnowflakeTableDataObject]].
 */
trait JdbcCatalogReferentialKeys extends CanHandleReferentialKeys { self: TransactionalTableDataObject =>

  protected def jCatalog: JdbcCatalog

  protected def fetchPrimaryKey(catalog: Option[String], schema: Option[String], tableName: String): Option[PrimaryKeyDefinition]

  protected def fetchForeignKeys(catalog: Option[String], schema: Option[String], tableName: String): Seq[ForeignKeyDefinition]

  protected def fetchColumnNullability(catalog: Option[String], schema: Option[String], tableName: String): Map[String, Boolean]

  // ── CanHandleReferentialKeys — concrete implementations ───────────────

  override def getExistingPKConstraint(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Option[PrimaryKeyDefinition] =
    fetchPrimaryKey(catalog, schema, tableName)

  override def dropPrimaryKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.dropPrimaryKeyConstraint(tableName, constraintName)

  override def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String])
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.createPrimaryKeyConstraint(tableName, constraintName, cols)

  override def ensureColumnsNotNull(tableName: String, columns: Seq[String])
      (implicit context: ActionPipelineContext): Unit = {
    val nullability = fetchColumnNullability(table.catalog, table.db, table.name)
    columns.filter(col => nullability.getOrElse(col, true)).foreach { col =>
      jCatalog.ensureColumnNotNull(tableName, col)
    }
  }

  override def getExistingFKConstraints(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition] =
    fetchForeignKeys(catalog, schema, tableName)

  override def dropForeignKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.dropForeignKeyConstraint(tableName, constraintName)

  override def createForeignKeyConstraint(tableName: String, constraintName: String,
      localColumns: Seq[String], refFullTableName: String, refColumns: Seq[String])
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.createForeignKeyConstraint(tableName, constraintName, localColumns, refFullTableName, refColumns)
}
