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
package io.smartdatalake.workflow.connection.jdbc

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.connection.authMode.{AuthMode, BasicAuthMode}
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata, EngineConnection}
import io.smartdatalake.workflow.dataobject.generic.{ForeignKeyDefinition, PrimaryKeyDefinition}
import org.apache.commons.pool2.impl.GenericObjectPool

import java.sql.{Connection => SqlConnection, DatabaseMetaData, DriverManager, ResultSet}
import scala.reflect.runtime.universe.Type
import scala.util.Try

/**
 * Connection information for JDBC tables. If authentication is needed, user and password must be
 * provided.
 *
 * It holds url, driver and credentials shared by all JdbcTableDataObjects referencing it through `connectionId`,
 * and maintains a small JDBC connection pool which SDLB uses for metadata and DDL statements (checking table
 * existence, creating tables, pre/postSQL, constraints). Note that Spark opens its own connections for reading
 * and writing data, so `maxParallelConnections` limits SDLB's own connections only. A database specific
 * [[JdbcCatalog]] is derived from `driver` to run catalog queries in the correct dialect.
 *
 * Example:
 * {{{
 * connections {
 *   jdbc-dwh {
 *     type = JdbcTableConnection
 *     url = "jdbc:postgresql://dwh.example.com:5432/dwh"
 *     driver = "org.postgresql.Driver"
 *     db = "public"
 *     authMode = {
 *       type = BasicAuthMode
 *       user = "###ENV#JDBC_USER###"
 *       password = "###ENV#JDBC_PASSWORD###"
 *     }
 *   }
 * }
 * }}}
 *
 * @note the JDBC driver named in `driver` must be on the classpath; only BasicAuthMode is supported as authMode.
 *
 * @param id
 *   unique id of this connection
 * @param url
 *   jdbc connection url
 * @param driver
 *   class name of jdbc driver
 * @param authMode
 *   optional authentication information: for now BasicAuthMode is supported.
 * @param db
 *   optional jdbc database to be used by tables having this connection assigned.
 * @param maxParallelConnections
 *   max number of parallel jdbc connections created by an instance of this connection, default is 3
 *   Note that Spark manages JDBC Connections on its own. This setting only applies to JDBC
 *   connection used by SDL for validating metadata or pre/postSQL.
 * @param connectionInitSql
 *   SQL statement to be executed every time a new connection is created, for example to set session
 *   parameters
 * @param directTableOverwrite
 *   flag to enable overwriting target tables directly without creating temporary table. Background:
 *   Spark uses multiple JDBC connections from different workers, this is done using multiple
 *   transactions. For SaveMode.Append this is ok, but it is problematic with SaveMode.Overwrite,
 *   where the table is truncated in a first transaction. Default is directTableWrite=false, this
 *   will write data first into a temporary table, and then use a "DELETE" + "INSERT INTO SELECT"
 *   statement to overwrite data in the target table within one transaction. Also note that
 *   SDLSaveMode.Merge always creates a temporary table.
 * @param connectionPool
 *   fine tuning of the JDBC connection pool used by SDLB, see [[ConnectionPoolConfig]], e.g. idle timeout and
 *   connection validation. Default is [[ConnectionPoolConfig]] with its default values.
 * @param dialect
 *   SQL dialect of the database as named by SQLGlot, e.g. postgres, tsql, oracle, snowflake or duckdb, see
 *   https://sqlglot.com/sqlglot/dialects.html. It is used by the SQL engine of sdl-sql to create SQL statements for
 *   the database. Default is to derive it from the JDBC url.
 */
case class JdbcTableConnection(
    override val id: ConnectionId,
    url: String,
    driver: String,
    authMode: Option[AuthMode] = None,
    db: Option[String] = None,
    maxParallelConnections: Int = 3,
    connectionInitSql: Option[String] = None,
    directTableOverwrite: Boolean = false,
    connectionPool: ConnectionPoolConfig = ConnectionPoolConfig(),
    dialect: Option[String] = None,
    override val metadata: Option[ConnectionMetadata] = None
) extends Connection with GenericJdbcExecution with EngineConnection with SmartDataLakeLogger {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(
    authMode.isEmpty || supportedAuths.contains(authMode.get.getClass),
    s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}."
  )

  // prepare catalog implementation
  val catalog: JdbcCatalog = JdbcCatalog.fromJdbcDriver(driver, this, url)
  // setup connection pool
  override val pool: GenericObjectPool[SqlConnection] = connectionPool
    .create(maxParallelConnections = maxParallelConnections, factoryFun = getConnection, initSql = connectionInitSql)

  def test(): Unit =
    execWithJdbcConnection(_ => ())

  /**
   * A JdbcTableConnection can be used as engine connection of an Action with the SQL engine of sdl-sql, which
   * executes the Action with SQL statements on the database. All input and output DataObjects of the Action must
   * then be JdbcTableDataObjects of this connection.
   */
  override def subFeedType: Type = JdbcTableConnection.sqlEngineSubFeedType

  /**
   * SQLGlot dialect of the database, see attribute `dialect`.
   */
  def sqlGlotDialect: String = dialect.orElse(JdbcTableConnection.dialectFromUrl(url))
    .getOrElse(throw ConfigurationException(s"($id) SQLGlot dialect can not be derived from JDBC url $url, please configure attribute dialect", Some(s"connections.$id.dialect")))

  private def getConnection(): SqlConnection = {
    Class.forName(driver)
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => DriverManager.getConnection(url, m.userSecret.resolve(), m.passwordSecret.resolve())
      case _                => throw new IllegalArgumentException(s"${authMode.getClass.getSimpleName} not supported.")
    }
    else DriverManager.getConnection(url)
  }

  def getAuthModeSparkOptions: Map[String, String] =
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => Map("user" -> m.userSecret.resolve(), "password" -> m.passwordSecret.resolve())
      case _                => throw new IllegalArgumentException(s"${authMode.getClass.getSimpleName} not supported.")
    }
    else Map()

  def dropTable(tableName: String, logging: Boolean = true): Unit =
    if (catalog.isTableExisting(tableName)) {
      execJdbcStatement(s"drop table $tableName", logging = logging)
    }

  private lazy val connectionMetadata: DatabaseMetaData = this.getConnection().getMetaData

  // The implementation to get the PK is not in the Catalog in order to use the JDBC standard method getPrimaryKeys
  // and not having to adapt the Query for different DBs.
  def getJdbcPrimaryKey(catalogOption: Option[String], schemaOption: Option[String], tableName: String): Option[PrimaryKeyDefinition] = {
    val resultSet: ResultSet = connectionMetadata.getPrimaryKeys(normalizeMetadataIdentifier(catalogOption).orNull,
      normalizeMetadataIdentifier(schemaOption).orNull, normalizeMetadataIdentifier(tableName))
    this.catalog.handlePrimaryKeyResultSet(resultSet)
  }

  /**
   * Normalize an identifier to the case the database stores it in, so that it can be used to query the JDBC
   * metadata, e.g. [[DatabaseMetaData.getColumns]]. Unquoted identifiers are stored uppercase by many
   * databases (e.g. HSQLDB, Oracle, SAP HANA) and lowercase by others (e.g. PostgreSQL), while quoted
   * identifiers are stored as written.
   */
  def normalizeMetadataIdentifier(identifier: String): String = {
    if (catalog.isQuotedIdentifier(identifier)) catalog.removeQuotes(identifier)
    else if (connectionMetadata.storesUpperCaseIdentifiers()) identifier.toUpperCase
    else if (connectionMetadata.storesLowerCaseIdentifiers()) identifier.toLowerCase
    else identifier
  }

  def normalizeMetadataIdentifier(identifier: Option[String]): Option[String] = identifier.map(normalizeMetadataIdentifier)

  // The implementation to get the foreign keys uses the JDBC standard method getImportedKeys,
  // so that the query doesn't need to be adapted for different DBs.
  def getJdbcForeignKeys(catalogOption: Option[String], schemaOption: Option[String], tableName: String): Seq[ForeignKeyDefinition] = {
    val resultSet: ResultSet = connectionMetadata.getImportedKeys(normalizeMetadataIdentifier(catalogOption).orNull,
      normalizeMetadataIdentifier(schemaOption).orNull, normalizeMetadataIdentifier(tableName))
    this.catalog.handleForeignKeyResultSet(resultSet)
  }

  /**
   * Read the comment of a table from the JDBC metadata (REMARKS).
   * Note that not all JDBC drivers return the comment of a table.
   */
  def getJdbcTableComment(schemaOption: Option[String], tableName: String): Option[String] = {
    val resultSet: ResultSet = connectionMetadata.getTables(null, normalizeMetadataIdentifier(schemaOption).orNull,
      normalizeMetadataIdentifier(tableName), null)
    try {
      if (resultSet.next()) Option(resultSet.getString("REMARKS")).filter(_.nonEmpty)
      else None
    } finally {
      resultSet.close()
    }
  }
}

object JdbcTableConnection extends FromConfigFactory[Connection] {

  private val sqlEngineSubFeedTypeName = "io.smartdatalake.workflow.dataframe.sql.SQLSubFeed"

  // the SubFeed type of the SQL engine, which is implemented in sdl-sql
  private lazy val sqlEngineSubFeedType: Type = Try(ReflectionUtil.classToType(Class.forName(sqlEngineSubFeedTypeName)))
    .getOrElse(throw ConfigurationException(s"Using a JdbcTableConnection as engine connection needs the SQL engine $sqlEngineSubFeedTypeName, please add sdl-sql to the classpath"))

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

  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableConnection =
    extract[JdbcTableConnection](config)
}
