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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.ConfigurationException
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.util.sqlglot.SqlGlotBridge
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.sql.{SQLDataFrame, SQLSubFeed}
import org.json4s.{DefaultFormats, Formats}

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * SQL engine implementation of creating the view of a [[JdbcViewDataObject]], see [[JdbcViewEngine]].
 * The view is created with a `CREATE OR REPLACE VIEW` statement rendered by SQLGlot in the dialect of the database,
 * from the query of the DataFrame, or from a query exported by a dry-run when deployed by CatalogSchemaUpdater.
 * Reading the view is done by [[JdbcTableSqlEngine]].
 *
 * A materialized view which can not be replaced by the database, e.g. on Postgres, is dropped and created again in
 * one transaction, and the privileges granted on it are granted again, see [[io.smartdatalake.workflow.connection.jdbc.JdbcCatalog.getGrants]].
 *
 * It is used by Actions with the JdbcConnection of the view as engine connection, see [[JdbcTableSqlEngine]].
 */
class JdbcViewSqlEngine(dataObject: JdbcViewDataObject) extends JdbcViewEngine with SmartDataLakeLogger {
  import dataObject.{connection, id, table}

  override def subFeedType: Type = typeOf[SQLSubFeed]

  private implicit val formats: Formats = DefaultFormats

  private def sqlDataFrame(df: GenericDataFrame)(implicit context: ActionPipelineContext): SQLDataFrame = {
    val engineConnection = SQLSubFeed.getEngineConnection
    if (!engineConnection.exists(_.id == connection.id)) throw ConfigurationException(
      s"($id) The SQL engine executes SQL statements on the database of its engine connection" +
        s" ${engineConnection.map(_.id.id).getOrElse("<none>")}, but this JdbcViewDataObject uses connection ${connection.id}." +
        " All inputs and outputs of an Action using the SQL engine must use its engine connection.")
    val sqlDf = SQLDataFrame.of(df)
    if (sqlDf.connection.id != connection.id) throw ConfigurationException(
      s"($id) Can not create a view from a DataFrame of connection ${sqlDf.connection.id} on connection ${connection.id}")
    sqlDf
  }

  /**
   * Validates the query of the view on the database, by executing it without fetching any rows.
   */
  override def initDataFrame(df: GenericDataFrame)(implicit context: ActionPipelineContext): Unit = {
    connection.execJdbcStatement(sqlDataFrame(df).limit(0).toDatabaseSql)
  }

  override def renderQuery(df: GenericDataFrame)(implicit context: ActionPipelineContext): String = sqlDataFrame(df).toDatabaseSql

  override def createOrReplaceView(query: String)(implicit context: ActionPipelineContext): Unit = {
    // the statement depends on whether the view exists, so that the grants on an existing view are kept
    val stmt = bridge.call("create_view", "query" -> query, "view" -> table.fullName, "dialect" -> connection.sqlGlotDialect,
      "exists" -> dataObject.isTableExisting).extract[String]
    connection.execJdbcStatement(stmt)
  }

  override def checkMaterializedViewSupported()(implicit context: ActionPipelineContext): Unit =
    bridge.call("check_materialized_view", "dialect" -> connection.sqlGlotDialect)

  override def createOrReplaceMaterializedView(query: String)(implicit context: ActionPipelineContext): Unit = {
    val stmts = bridge.call("create_materialized_view", "query" -> query, "view" -> table.fullName,
      "dialect" -> connection.sqlGlotDialect, "exists" -> dataObject.isTableExisting)
    val create = (stmts \ "create").extract[String]
    (stmts \ "drop").extractOpt[String] match {
      case None => connection.execJdbcStatement(create)
      case Some(drop) => recreateMaterializedView(drop, create)
    }
  }

  /**
   * Drop and create the materialized view, and grant the privileges on it again, all in one transaction.
   */
  private def recreateMaterializedView(drop: String, create: String): Unit = {
    val grants = connection.catalog.getGrants(table.db.get, table.name)
    if (grants.isEmpty) logger.warn(s"($id) the privileges granted on materialized view ${table.fullName} can not be read" +
      s" for this database, they are lost as it is dropped and created again")
    val grantStmts = grants.map(connection.catalog.grantStatements(table.fullName, _)).getOrElse(Seq())
    val transaction = connection.beginTransaction()
    try {
      (Seq(drop, create) ++ grantStmts).foreach(transaction.execJdbcStatement(_))
      transaction.commit()
    } catch {
      case e: Exception =>
        transaction.rollback()
        throw e
    }
    if (grantStmts.nonEmpty) logger.info(s"($id) granted privileges on materialized view ${table.fullName} again: " +
      grants.get.map(g => s"${g.privilege} to ${g.grantee}").mkString(", "))
  }

  override def refreshMaterializedView()(implicit context: ActionPipelineContext): Unit =
    bridge.call("refresh_materialized_view", "view" -> table.fullName, "dialect" -> connection.sqlGlotDialect).extractOpt[String] match {
      case Some(stmt) => connection.execJdbcStatement(stmt)
      case None => logger.info(s"($id) materialized view ${table.fullName} is refreshed automatically by the database")
    }

  override def normalizeQuery(query: String)(implicit context: ActionPipelineContext): String =
    bridge.call("normalize_query", "query" -> query, "dialect" -> connection.sqlGlotDialect).extract[String]

  private def bridge: SqlGlotBridge = SqlGlotBridge.get()
}
