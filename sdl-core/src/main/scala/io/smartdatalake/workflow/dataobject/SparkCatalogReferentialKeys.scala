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

import io.smartdatalake.util.spark.SparkQueryUtil
import io.smartdatalake.workflow.ActionPipelineContext

/**
 * Concrete implementation of [[CanHandleReferentialKeys]] that uses Spark SQL against a Unity
 * Catalog-backed catalog (Databricks).  Shared by [[DeltaLakeTableDataObject]] and
 * [[IcebergTableDataObject]] to avoid duplicating the INFORMATION_SCHEMA queries and ALTER TABLE DDL.
 *
 * All statements are executed via [[SparkQueryUtil.executeSqlStatementBasedOnTable]], which
 * automatically prepends `USE CATALOG` / `USE SCHEMA` context from the [[Table]] definition.
 */
trait SparkCatalogReferentialKeys extends CanHandleReferentialKeys { self: TransactionalTableDataObject =>

  // ── NOT NULL ──────────────────────────────────────────────────────────

  override def ensureColumnsNotNull(tableName: String, columns: Seq[String])
      (implicit context: ActionPipelineContext): Unit = {
    val schema = context.sparkSession.table(tableName).schema
    columns.filter(col => schema.fields.exists(f => f.name.equalsIgnoreCase(col) && f.nullable)).foreach { col =>
      val query = s"ALTER TABLE $tableName ALTER COLUMN $col SET NOT NULL"
      SparkQueryUtil.executeSqlStatementBasedOnTable(context.sparkSession, query, table)
    }
  }

  // ── Primary Key ───────────────────────────────────────────────────────

  override def getExistingPKConstraint(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Option[PrimaryKeyDefinition] = {
    val catalogConstraint = catalog.map(c => s" and TABLE_CATALOG = '$c'").getOrElse("")
    val schemaConstraint = schema.map(s => s" and TABLE_SCHEMA = '$s'").getOrElse("")
    val baseQuery = s"select COLUMN_NAME, CONSTRAINT_NAME as PK_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_NAME = '$tableName'"
    val query = Seq(baseQuery, schemaConstraint, catalogConstraint).mkString.toLowerCase
    val df = context.sparkSession.sql(query)
    val (primaryKeyCols, primaryKeyName) = df.collect().foldLeft(Set[String](), Set[String]()) {
      (sets, row) => (sets._1 + row.getString(0), sets._2 + row.getString(1))
    }
    import java.sql.SQLException
    (primaryKeyCols.toList, primaryKeyName.toList) match {
      case (Nil, _) => None
      case (cols, Nil) => Some(PrimaryKeyDefinition(cols))
      case (_, pk) if pk.size > 1 => throw new SQLException(s"Table $tableName has more than one primary key: ${pk.mkString}")
      case (cols, pk) => Some(PrimaryKeyDefinition(cols, Some(pk.head)))
    }
  }

  override def dropPrimaryKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit =
    dropConstraint(tableName, constraintName)

  override def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String])
      (implicit context: ActionPipelineContext): Unit = {
    val query = s"ALTER TABLE $tableName ADD CONSTRAINT $constraintName PRIMARY KEY (${cols.mkString(",")}) RELY"
    SparkQueryUtil.executeSqlStatementBasedOnTable(context.sparkSession, query, table)
  }

  // ── Foreign Key ───────────────────────────────────────────────────────

  override def getExistingFKConstraints(catalog: Option[String], schema: Option[String], tableName: String)
      (implicit context: ActionPipelineContext): Seq[ForeignKeyDefinition] = {
    val catalogPrefix = catalog.map(c => s"$c.").getOrElse("")
    val schemaFilter = schema.map(s => s" AND tc.TABLE_SCHEMA = '$s'").getOrElse("")
    val query =
      s"""SELECT tc.CONSTRAINT_NAME, kcu.COLUMN_NAME AS LOCAL_COLUMN, kcu.ORDINAL_POSITION,
         |       ccu.TABLE_CATALOG AS REF_CATALOG, ccu.TABLE_SCHEMA AS REF_SCHEMA,
         |       ccu.TABLE_NAME AS REF_TABLE, ccu.COLUMN_NAME AS REF_COLUMN
         |FROM ${catalogPrefix}INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
         |JOIN ${catalogPrefix}INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
         |  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         |  AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
         |  AND tc.TABLE_NAME = kcu.TABLE_NAME
         |JOIN ${catalogPrefix}INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
         |  ON tc.CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
         |WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
         |  AND tc.TABLE_NAME = '$tableName'$schemaFilter""".stripMargin
    val df = context.sparkSession.sql(query.toLowerCase)
    df.collect()
      .groupBy(row => row.getString(row.fieldIndex("constraint_name")))
      .map { case (constraintName, rows) =>
        val ordered = rows.sortBy(row => row.getInt(row.fieldIndex("ordinal_position")))
        val head = ordered.head
        ForeignKeyDefinition(
          constraintName = constraintName,
          localColumns = ordered.map(r => r.getString(r.fieldIndex("local_column"))),
          refCatalog = Option(head.getString(head.fieldIndex("ref_catalog"))).filter(_.nonEmpty),
          refSchema = Option(head.getString(head.fieldIndex("ref_schema"))).filter(_.nonEmpty),
          refTable = head.getString(head.fieldIndex("ref_table")),
          refColumns = ordered.map(r => r.getString(r.fieldIndex("ref_column")))
        )
      }.toSeq
  }

  override def dropForeignKeyConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit =
    dropConstraint(tableName, constraintName)

  private def dropConstraint(tableName: String, constraintName: String)
      (implicit context: ActionPipelineContext): Unit = {
    val query = s"ALTER TABLE $tableName DROP CONSTRAINT $constraintName"
    SparkQueryUtil.executeSqlStatementBasedOnTable(context.sparkSession, query, table)
  }

  override def createForeignKeyConstraint(tableName: String, constraintName: String,
      localColumns: Seq[String], refFullTableName: String, refColumns: Seq[String])
      (implicit context: ActionPipelineContext): Unit = {
    val query = s"ALTER TABLE $tableName ADD CONSTRAINT $constraintName FOREIGN KEY (${localColumns.mkString(",")}) REFERENCES $refFullTableName (${refColumns.mkString(",")}) RELY"
    SparkQueryUtil.executeSqlStatementBasedOnTable(context.sparkSession, query, table)
  }
}
