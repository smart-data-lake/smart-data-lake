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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry, SdlConfigObject}
import io.smartdatalake.util.misc.{ConnectionPoolConfig, GenericJdbcExecution, SmartDataLakeLogger}
import io.smartdatalake.util.sqlglot.SqlGlotBridge
import io.smartdatalake.workflow.connection.authMode.{AuthMode, BasicAuthMode}
import io.smartdatalake.workflow.dataframe.sql.SQLSubFeed
import org.apache.commons.pool2.impl.GenericObjectPool

import java.sql.{DriverManager, Connection => SqlConnection}
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Engine connection for the SQL engine, and JDBC connection to the database executing its SQL statements.
 *
 * The SQL engine does not process data itself, but creates SQL statements to be executed by the database (ELT).
 * DataFrame operations and SQL transformers are translated into an SQLGlot query, which runs in a Python interpreter
 * embedded into the JVM with jep. SQLGlot optimizes the query and renders it in the SQL dialect of the database.
 *
 * The Python environment must have sqlglot and jep installed, see sdl-sql/pyproject.toml.
 *
 * Example:
 * {{{
 * connections {
 *   sql-engine {
 *     type = SQLEngineConnection
 *     url = "jdbc:postgresql://localhost:5432/db"
 *     authMode {
 *       type = BasicAuthMode
 *       userVariable = "ENV#DB_USER"
 *       passwordVariable = "ENV#DB_PASSWORD"
 *     }
 *     sqlDialect = spark
 *   }
 * }
 * }}}
 *
 * @param id                     unique id of this connection
 * @param url                    JDBC url of the database
 * @param driver                 class name of the JDBC driver. It is only needed for drivers that do not register
 *                               themselves with the JDBC DriverManager.
 * @param authMode               optional authentication information, only BasicAuthMode is supported.
 * @param dialect                SQLGlot dialect of the database, e.g. postgres, tsql, oracle, snowflake or duckdb.
 *                               See https://sqlglot.com/sqlglot/dialects.html for the supported dialects.
 *                               Default is to derive it from the JDBC url.
 * @param sqlDialect             SQLGlot dialect of the SQL queries of SQL transformers. Default is the dialect of the
 *                               database. Set it to `spark` to reuse transformations written for Spark.
 * @param pythonExecutable       Python executable of the environment with sqlglot and jep installed. Default is the
 *                               environment variable SDLB_PYTHON, or python3 on the PATH. Note that the Python runtime
 *                               can only be initialized once per JVM, so all SQLEngineConnections use the same environment.
 * @param maxParallelConnections max number of parallel JDBC connections, default is 3
 * @param connectionInitSql      SQL statement to be executed every time a new JDBC connection is created, for example
 *                               to set session parameters
 * @param connectionPool         fine tuning of the JDBC connection pool, see [[ConnectionPoolConfig]]
 * @param metadata               additional metadata for this connection (name, description, layer, ...)
 */
case class SQLEngineConnection(
                                id: SdlConfigObject.ConnectionId,
                                url: String,
                                driver: Option[String] = None,
                                authMode: Option[AuthMode] = None,
                                dialect: Option[String] = None,
                                sqlDialect: Option[String] = None,
                                pythonExecutable: Option[String] = None,
                                maxParallelConnections: Int = 3,
                                connectionInitSql: Option[String] = None,
                                connectionPool: ConnectionPoolConfig = ConnectionPoolConfig(),
                                metadata: Option[ConnectionMetadata] = None
                              ) extends Connection with EngineConnection with GenericJdbcExecution with SmartDataLakeLogger {

  require(authMode.isEmpty || authMode.get.isInstanceOf[BasicAuthMode],
    s"($id) ${authMode.get.getClass.getSimpleName} not supported by ${getClass.getSimpleName}, only BasicAuthMode is supported")

  override def subFeedType: Type = typeOf[SQLSubFeed]

  /**
   * SQLGlot dialect of the database
   */
  val databaseDialect: String = dialect.getOrElse(SQLEngineConnection.dialectFromUrl(url)
    .getOrElse(throw ConfigurationException(s"($id) SQLGlot dialect can not be derived from JDBC url $url, please configure attribute dialect", Some(s"connections.$id.dialect"))))

  /**
   * SQLGlot dialect of the SQL queries of SQL transformers
   */
  def queryDialect: String = sqlDialect.getOrElse(databaseDialect)

  /**
   * The SQLGlot bridge of the embedded Python interpreter, created on first use.
   */
  def bridge: SqlGlotBridge = SqlGlotBridge.get(pythonExecutable)

  @transient private var poolInitialized = false
  override lazy val pool: GenericObjectPool[SqlConnection] = {
    poolInitialized = true
    connectionPool.create(maxParallelConnections, () => getConnection, connectionInitSql)
  }

  private def getConnection: SqlConnection = {
    driver.foreach(Class.forName)
    authMode match {
      case Some(m: BasicAuthMode) => DriverManager.getConnection(url, m.userSecret.resolve(), m.passwordSecret.resolve())
      case _ => DriverManager.getConnection(url)
    }
  }

  override def close(): Unit = {
    if (poolInitialized) pool.close()
    SqlGlotBridge.close()
  }
}

object SQLEngineConnection extends FromConfigFactory[Connection] {

  // sub protocol of JDBC urls and the corresponding SQLGlot dialect
  private val dialectBySubProtocol = Map(
    "postgresql" -> "postgres", "sqlserver" -> "tsql", "oracle" -> "oracle", "mysql" -> "mysql", "mariadb" -> "mysql",
    "duckdb" -> "duckdb", "snowflake" -> "snowflake", "sqlite" -> "sqlite", "redshift" -> "redshift", "trino" -> "trino",
    "presto" -> "presto", "databricks" -> "databricks", "clickhouse" -> "clickhouse", "teradata" -> "teradata",
    "bigquery" -> "bigquery", "exasol" -> "exasol"
  )

  /**
   * Derive the SQLGlot dialect from the sub protocol of a JDBC url, e.g. `jdbc:postgresql://...` -> postgres
   */
  def dialectFromUrl(url: String): Option[String] =
    url.split(':').toSeq match {
      case Seq("jdbc", subProtocol, _*) => dialectBySubProtocol.get(subProtocol.toLowerCase)
      case _ => None
    }

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): SQLEngineConnection = {
    extract[SQLEngineConnection](config)
  }
}
