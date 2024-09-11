/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.connection.authMode.{AuthMode, BasicAuthMode}
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getJdbcType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{Set => MutableSet}
import java.sql.{DatabaseMetaData, DriverManager, ResultSet, SQLException, Connection => SqlConnection}

/**
 * Connection information for JDBC tables.
 * If authentication is needed, user and password must be provided.
 *
 * @param id unique id of this connection
 * @param url jdbc connection url
 * @param driver class name of jdbc driver
 * @param authMode optional authentication information: for now BasicAuthMode is supported.
 * @param db optional jdbc database to be used by tables having this connection assigned.
 * @param maxParallelConnections max number of parallel jdbc connections created by an instance of this connection, default is 3
 *                               Note that Spark manages JDBC Connections on its own. This setting only applies to JDBC connection
 *                               used by SDL for validating metadata or pre/postSQL.
 * @param connectionPoolMaxIdleTimeSec timeout to close unused connections in the pool. Deprecated: Use connectionPool.maxIdleTimeSec instead.
 * @param connectionPoolMaxWaitTimeSec timeout when waiting for connection in pool to become available. Deprecated: Use connectionPool.maxWaitTimeSec instead.
 * @param autoCommit flag to enable or disable the auto-commit behaviour. When autoCommit is enabled, each database request is executed in its own transaction.
 *                   Default is autoCommit = false. It is not recommended to enable autoCommit as it will deactivate any transactional behaviour.
 * @param connectionInitSql SQL statement to be executed every time a new connection is created, for example to set session parameters
 * @param directTableOverwrite flag to enable overwriting target tables directly without creating temporary table.
 *                         Background: Spark uses multiple JDBC connections from different workers, this is done using multiple transactions.
 *                         For SaveMode.Append this is ok, but it is problematic with SaveMode.Overwrite, where the table is truncated in a first transaction.
 *                         Default is directTableWrite=false, this will write data first into a temporary table, and then use
 *                         a "DELETE" + "INSERT INTO SELECT" statement to overwrite data in the target table within one transaction.
 *                         Also note that SDLSaveMode.Merge always creates a temporary table.
 */
case class JdbcTableConnection(override val id: ConnectionId,
                               url: String,
                               driver: String,
                               authMode: Option[AuthMode] = None,
                               db: Option[String] = None,
                               maxParallelConnections: Int = 3,
                               @Deprecated @deprecated("Use connectionPool.maxIdleTimeSec instead.", "2.6.1")
                               connectionPoolMaxIdleTimeSec: Option[Int] = None,
                               @Deprecated @deprecated("Use connectionPool.maxWaitTimeSec instead.", "2.6.1")
                               connectionPoolMaxWaitTimeSec: Option[Int] = None,
                               @Deprecated @deprecated("Enabling autoCommit is no longer recommended.", "2.5.0")
                               override val autoCommit: Boolean = false,
                               connectionInitSql: Option[String] = None,
                               directTableOverwrite: Boolean = false,
                               connectionPool: ConnectionPoolConfig = ConnectionPoolConfig(),
                               override val metadata: Option[ConnectionMetadata] = None,
                               ) extends Connection with JdbcExecution with SmartDataLakeLogger {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(authMode.isEmpty || supportedAuths.contains(authMode.get.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  // prepare catalog implementation
  val catalog: JdbcCatalog = JdbcCatalog.fromJdbcDriver(driver, this)
  // setup connection pool
  override val pool: GenericObjectPool[SqlConnection] = connectionPool
    .withOverride(connectionPoolMaxIdleTimeSec, connectionPoolMaxWaitTimeSec)
    .create(maxParallelConnections, getConnection _, connectionInitSql, autoCommit)
  override val jdbcDialect: JdbcDialect = JdbcDialects.get(url)

  def test(): Unit = {
    execWithJdbcConnection{ _ => () }
  }

  private def getConnection: SqlConnection = {
    Class.forName(driver)
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => DriverManager.getConnection(url, m.userSecret.resolve(), m.passwordSecret.resolve())
      case _ => throw new IllegalArgumentException(s"${authMode.getClass.getSimpleName} not supported.")
    } else DriverManager.getConnection(url)
  }

  def getAuthModeSparkOptions: Map[String,String] = {
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => Map( "user" -> m.userSecret.resolve(), "password" -> m.passwordSecret.resolve())
      case _ => throw new IllegalArgumentException(s"${authMode.getClass.getSimpleName} not supported.")
    } else Map()
  }

  /**
   * Code partly copied from Spark:JdbcUtils to adapt schemaString method to not quote identifiers if Spark is in case-insensitive mode.
   */
  def createTableFromSchema(tableName: String, schema: StructType, rawOptions: Map[String,String])(implicit session: SparkSession): Unit = {
    def schemaString(
                      schema: StructType,
                      caseSensitive: Boolean,
                      url: String,
                      createTableColumnTypes: Option[String] = None): String = {
      val sb = new StringBuilder()
      val dialect = JdbcDialects.get(url)
      val userSpecifiedColTypesMap = createTableColumnTypes
        .map(parseUserSpecifiedCreateTableColumnTypes(schema, caseSensitive, _))
        .getOrElse(Map.empty[String, String])
      schema.fields.foreach { field =>
        // Change is here - dont quote if not case-sensitive and normal characters used:
        val name = if(caseSensitive || SQLUtil.hasIdentifierSpecialChars(field.name)) dialect.quoteIdentifier(field.name)
          else field.name
        val typ = userSpecifiedColTypesMap
          .getOrElse(field.name, getJdbcType(field.dataType, dialect).databaseTypeDefinition)
        val nullable = if (field.nullable) "" else "NOT NULL"
        sb.append(s", $name $typ $nullable")
      }
      if (sb.length < 2) "" else sb.substring(2)
    }
    def parseUserSpecifiedCreateTableColumnTypes(schema: StructType, caseSensitive: Boolean, createTableColumnTypes: String): Map[String, String] = {
      val userSchema = CatalystSqlParser.parseTableSchema(createTableColumnTypes)
      val userSchemaMap = userSchema.fields.map(f => f.name -> f.dataType.catalogString).toMap
      if (caseSensitive) userSchemaMap else CaseInsensitiveMap(userSchemaMap)
    }
    val options =  new JdbcOptionsInWrite(url, tableName, rawOptions)
    val strSchema = schemaString(schema, Environment.caseSensitive, options.url, options.createTableColumnTypes)
    val createTableOptions = options.createTableOptions
    val sql = s"CREATE TABLE $tableName ($strSchema) $createTableOptions"
    execJdbcStatement(sql)
  }

  def dropTable(tableName: String, logging: Boolean = true): Unit = {
    if (catalog.isTableExisting(tableName)) {
      execJdbcStatement(s"drop table $tableName", logging = logging)
    }
  }

  case class JdbcPrimaryKey(pkColumns: Seq[String], pkName: Option[String] = None)

  private lazy val connectionMetadata: DatabaseMetaData = this.getConnection.getMetaData
  def getJdbcPrimaryKey(catalog: String, schema: String, tableName: String): Option[JdbcPrimaryKey] = {
    var resultSet: ResultSet = connectionMetadata.getPrimaryKeys(catalog, schema, tableName)
    var primaryKeyCols: MutableSet[String] = MutableSet()
    var primaryKeyName: MutableSet[String] = MutableSet()
    while (resultSet.next()) {
      primaryKeyCols += resultSet.getString("COLUMN_NAME")
      primaryKeyName += resultSet.getString("PK_NAME")
    }
    (primaryKeyCols.toList, primaryKeyName.toList) match {
      case (List(), _) => None
      case (cols, List()) => Some(JdbcPrimaryKey(cols))
      case (_, pk) if pk.size > 1 => throw new SQLException(f"The JDBC-Connection for $tableName returns more than one Primary Key!")
      case (cols, pk) => Some(JdbcPrimaryKey(cols, Some(pk.head)))
    }
  }

  override def factory: FromConfigFactory[Connection] = JdbcTableConnection
}

object JdbcTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableConnection = {
    extract[JdbcTableConnection](config)
  }
}

