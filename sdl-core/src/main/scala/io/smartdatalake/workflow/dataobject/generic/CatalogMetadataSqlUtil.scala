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

import io.smartdatalake.util.misc.{SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchemaUtil}

import scala.util.Try

/**
 * Implementation of [[CanHandleCatalogMetadata]] for catalogs using Spark SQL syntax, e.g. Delta Lake
 * and Iceberg tables.
 *
 * All statements address the table by its fully qualified name and none of them changes the current
 * catalog or schema of the session, see [[CanHandleCatalogMetadata]].
 */
object CatalogMetadataSqlUtil extends SmartDataLakeLogger {

  /**
   * Row of "DESCRIBE TABLE EXTENDED" holding the table comment.
   */
  private val tableCommentAttribute = "Comment"

  /**
   * Read the table comment using "DESCRIBE TABLE EXTENDED", which lists it as an attribute row
   * named "Comment". Returns None if the table has no comment.
   */
  def getTableComment(table: Table, sql: String => GenericDataFrame): Option[String] = {
    val rows = sql(s"DESCRIBE TABLE EXTENDED ${table.fullName}").collect
    rows
      .find(row => Try(row.getAs[String](0)).toOption.contains(tableCommentAttribute))
      .flatMap(row => Try(row.getAs[String](1)).toOption)
      .filter(_ != null)
  }

  def setTableComment(table: Table, comment: String, sql: String => Unit, loggerContext: String): Unit = {
    val stmt = s"ALTER TABLE ${table.fullName} SET TBLPROPERTIES ('comment' = '${SQLUtil.escapeSqlStringLiteral(comment)}')"
    SQLUtil.execSql(stmt, sql, loggerContext)
  }

  def setColumnComments(table: Table, comments: Map[Seq[String], String], sql: String => Unit, loggerContext: String): Unit = {
    comments.foreach { case (columnPath, comment) =>
      val stmt = s"ALTER TABLE ${table.fullName} ALTER COLUMN ${GenericSchemaUtil.formatColumnPath(columnPath)}" +
        s" COMMENT '${SQLUtil.escapeSqlStringLiteral(comment)}'"
      SQLUtil.execSql(stmt, sql, loggerContext)
    }
  }
}
