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

package io.smartdatalake.util.spark

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.{SparkSession, DataFrame}
import io.smartdatalake.workflow.dataobject.Table

object SparkQueryUtil extends SmartDataLakeLogger {
  /**
   * This method is used to execute SQL-statements configured at the DataObject-level.
   * In order to avoid using another catalog that is not explicitly stated in the SQL-Statement,
   * the catalogs and schemas of the given DataObject are set as default.
   * @param session Spark Session
   * @param stmt Desired SQL statement to be executed.
   * @param table DataObject in which the SQLStatement is configured
   */
  def executeSqlStatementBasedOnTable(session: SparkSession, stmt: String, table: Table): Unit = {
    try {
      val newStmt = Seq(
        table.catalog.map(cat => s"USE CATALOG $cat"),
        table.db.map(db => s"USE SCHEMA $db"),
        Some(stmt)
      ).flatten
      logger.info(s"Executing SQL statements: ${newStmt.mkString(";")}")
      newStmt.foreach(session.sql)
    } catch {
      case e: Exception =>
        logger.warn(s"Error in SQL statement '$stmt':\n${e.getMessage}")
        throw e
    }
  }

  def executeSqlQuery(session: SparkSession, query: String, abortRunIfException: Boolean = false): Option[DataFrame] = {
    logger.info(s"Executing query $query")
    try {
      Some(session.sql(query))
    }
    catch {
      case e: Exception if abortRunIfException =>
        logger.error(f"Error in SQL statement '$query')")
        throw e
      case e: Exception =>
        logger.warn(f"Error in SQL statement '$query'):\n${e.getMessage}")
        None
    }
  }
}
