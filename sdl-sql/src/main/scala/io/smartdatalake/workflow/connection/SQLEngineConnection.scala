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
package io.smartdatalake.workflow.connection

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.util.sqlglot.SqlGlotBridge
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Engine connection for the SQL engine.
 *
 * The SQL engine does not process data itself, but creates SQL statements to be executed by the database (ELT).
 * DataFrame operations and SQL transformers are translated into an SQLGlot query, which runs in a Python interpreter
 * embedded into the JVM with jep. SQLGlot optimizes the query and renders it in the SQL dialect of the database.
 *
 * The Python environment must have sqlglot and jep installed, see sdl-sql/pyproject.toml.
 *
 * Note that executing the SQL statements on the database is not implemented yet (see issue #866).
 *
 * Example:
 * {{{
 * connections {
 *   sql-engine {
 *     type = SQLEngineConnection
 *     dialect = postgres
 *     sqlDialect = spark
 *   }
 * }
 * }}}
 *
 * @param id               unique id of this connection
 * @param dialect          SQLGlot dialect of the database, e.g. postgres, tsql, oracle, snowflake or duckdb.
 *                         See https://sqlglot.com/sqlglot/dialects.html for the supported dialects.
 * @param sqlDialect       SQLGlot dialect of the SQL queries of SQL transformers. Default is `dialect`.
 *                         Set it to `spark` to reuse transformations written for Spark.
 * @param pythonExecutable Python executable of the environment with sqlglot and jep installed. Default is the environment
 *                         variable SDLB_PYTHON, or python3 on the PATH. Note that the Python runtime can only be
 *                         initialized once per JVM, so all SQLEngineConnections use the same environment.
 * @param metadata         additional metadata for this connection (name, description, layer, ...)
 */
case class SQLEngineConnection(
                                id: SdlConfigObject.ConnectionId,
                                dialect: String,
                                sqlDialect: Option[String] = None,
                                pythonExecutable: Option[String] = None,
                                metadata: Option[ConnectionMetadata] = None
                              ) extends Connection with EngineConnection {

  override def subFeedType: Type = typeOf[SQLSubFeed]

  /**
   * The SQLGlot bridge of the embedded Python interpreter, created on first use.
   */
  def bridge: SqlGlotBridge = SqlGlotBridge.get(pythonExecutable)

  override def close(): Unit = SqlGlotBridge.close()
}

object SQLEngineConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLEngineConnection = {
    extract[SQLEngineConnection](config)
  }
}
