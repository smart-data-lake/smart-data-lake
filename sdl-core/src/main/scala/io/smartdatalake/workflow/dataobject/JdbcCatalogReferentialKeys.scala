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
import io.smartdatalake.workflow.connection.jdbc.{DefaultJdbcCatalog, JdbcCatalog}

/**
 * Concrete implementation of [[CanHandleReferentialKeys]] for JDBC-backed DataObjects.
 *
 * `jCatalog` is the single abstract hook — it provides the JDBC DDL operations
 * (create / drop constraints, set NOT NULL).  Query operations (read existing PKs / FKs /
 * column nullability) are dispatched to [[DefaultJdbcCatalog]] when available; custom
 * catalog types can extend [[DefaultJdbcCatalog]] and override those methods.
 *
 * Other catalog technologies (REST, Hive, …) implement [[CanHandleReferentialKeys]] directly
 * and are independent of this trait.
 *
 * Used by [[JdbcTableDataObject]] and [[SnowflakeTableDataObject]]:
 * {{{
 *   override protected def jCatalog = connection.catalog
 * }}}
 */
trait JdbcCatalogReferentialKeys extends CanHandleReferentialKeys { self: TransactionalTableDataObject =>

  /** JDBC catalog used for DDL operations (create / drop constraints, set NOT NULL). */
  protected def jCatalog: JdbcCatalog

  // ── CanHandleReferentialKeys — DDL operations via jCatalog ────────────

  override def dropPrimaryKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.dropPrimaryKeyConstraint(tableName, constraintName)

  override def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String])
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.createPrimaryKeyConstraint(tableName, constraintName, cols)

  override def ensureColumnsNotNull(tableName: String, columns: Seq[String])
      (implicit context: ActionPipelineContext): Unit = {
    val nullability = requireDefaultCatalog("getColumnNullability").getColumnNullability(table.catalog, table.db, table.name)
    columns.filter(col => nullability.getOrElse(col, true)).foreach { col =>
      jCatalog.ensureColumnNotNull(tableName, col)
    }
  }

  override def dropForeignKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.dropForeignKeyConstraint(tableName, constraintName)

  override def createForeignKeyConstraint(tableName: String, constraintName: String,
      localColumns: Seq[String], refFullTableName: String, refColumns: Seq[String])
      (implicit context: ActionPipelineContext): Unit =
    jCatalog.createForeignKeyConstraint(tableName, constraintName, localColumns, refFullTableName, refColumns)

  // ── CanHandleReferentialKeys — query operations via DefaultJdbcCatalog ─

  override def getExistingPKConstraint(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Option[PrimaryKeyDefinition] =
    requireDefaultCatalog("getPrimaryKey").getPrimaryKey(catalog, schema, tableName)

  override def getExistingFKConstraints(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition] =
    requireDefaultCatalog("getForeignKeys").getForeignKeys(catalog, schema, tableName)

  // ── Private helper ────────────────────────────────────────────────────

  private def requireDefaultCatalog(op: String): DefaultJdbcCatalog = jCatalog match {
    case dc: DefaultJdbcCatalog => dc
    case other => throw new UnsupportedOperationException(
      s"$id: $op requires a DefaultJdbcCatalog (INFORMATION_SCHEMA) but catalog is ${other.getClass.getSimpleName}. " +
      s"Override $op in a custom JdbcCatalog subclass to add support.")
  }
}
