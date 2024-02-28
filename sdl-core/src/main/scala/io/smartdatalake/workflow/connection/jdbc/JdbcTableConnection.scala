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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode, Environment}
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getJdbcType
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import java.sql.{DriverManager, ResultSet, Statement, Connection => SqlConnection}

/**
 * Connection information for jdbc tables.
 * If authentication is needed, user and password must be provided.
 *
 * @param id unique id of this connection
 * @param url jdbc connection url
 * @param driver class name of jdbc driver
 * @param authMode optional authentication information: for now BasicAuthMode is supported.
 * @param db jdbc database
 * @param maxParallelConnections max number of parallel jdbc connections created by an instance of this connection, default is 3
 *                               Note that Spark manages JDBC Connections on its own. This setting only applies to JDBC connection
 *                               used by SDL for validating metadata or pre/postSQL.
 * @param connectionPoolMaxIdleTimeSec timeout to close unused connections in the pool
 * @param connectionPoolMaxWaitTimeSec timeout when waiting for connection in pool to become available. Default is 600 seconds (10 minutes).
 * @param autoCommit flag to enable or disable the auto-commit behaviour. When autoCommit is enabled, each database request is executed in its own transaction.
 *                   Default is autoCommit = false. It is not recommended to enable autoCommit as it will deactivate any transactional behaviour.
 * @param connectionInitSql SQL statement to be executed every time a new connection is created, for example to set session parameters
 * @param connectionPoolClassName Optional class name of an implementation of JdbcConnectionPool. By default JdbcConnectionPoolImpl is used.
 *                                In addition JdbcConnectionPool implementations must also have a constructor with a JdbcTableConnection as argument.
 * @param directTableOverwrite flag to enable overwriting target tables directly without creating temporary table.
 *                         Background: Spark uses multiple JDBC connections from different workers, this is done using multiple transactions.
 *                         For SaveMode.Append this is ok, but it problematic with SaveMode.Overwrite, where the table is truncated in a first transaction.
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
                               connectionPoolMaxIdleTimeSec: Int = 3,
                               connectionPoolMaxWaitTimeSec: Int = 600,
                               @Deprecated @deprecated("Enabling autoCommit is no longer recommended.", "2.5.0") autoCommit: Boolean = false,
                               connectionInitSql: Option[String] = None,
                               connectionPoolClassName: Option[String] = None,
                               directTableOverwrite: Boolean = false,
                               override val metadata: Option[ConnectionMetadata] = None,
                               ) extends Connection with SmartDataLakeLogger {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(authMode.isEmpty || supportedAuths.contains(authMode.get.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  // prepare catalog implementation
  val catalog: JdbcCatalog = JdbcCatalog.fromJdbcDriver(driver, this)

  // connection pool - instantiate using connectionPoolClassName if defined, otherwise use JdbcConnectionPoolImpl
  val pool: ConnectionPool[SqlConnection] = connectionPoolClassName.map(className =>
    try {
      val clazz = Environment.classLoader().loadClass(className)
      val constructor = clazz.getConstructor(classOf[JdbcTableConnection])
      constructor.newInstance(this).asInstanceOf[ConnectionPool[SqlConnection]]
    } catch {
      case e: NoSuchMethodException => throw ConfigurationException(s"($id) Connection Pool class $className needs constructor with one parameter of type JdbcConnectionPool: ${e.getMessage}", Some("connectionPoolClassName"), e)
      case e: Exception => throw ConfigurationException(s"($id) Cannot instantiate Connection Pool class $className: ${e.getMessage}", Some("connectionPoolClassName"), e)
    }
  ).getOrElse(new DefaultConnectionPool[SqlConnection](maxParallelConnections, connectionPoolMaxIdleTimeSec, connectionPoolMaxWaitTimeSec, Unit => JdbcConnectionFactory.getConnection(this), _.close))

  /**
   * Get a connection from the pool and execute an arbitrary function
   */
  def execWithJdbcConnection[A]( func: SqlConnection => A ): A = {
    WithResourcePool.exec(pool){
      con => func(con)
    }
  }

  private def execWithJdbcStatement[A](conn: SqlConnection, doCommit: Boolean)(func: Statement => A): A = {
    var stmt: Statement = null
    try {
      stmt = conn.createStatement
      val result = func(stmt)
      if (doCommit && !autoCommit) conn.commit()
      result
    } catch {
      case e: Exception =>
        conn.rollback()
        throw e
    } finally {
      if (stmt != null) stmt.close()
    }
  }

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary sql statement
   * @return true if the first result is a ResultSet object; false if it is an update count or there are no results (see also Jdbc.Statement.execute())
   */
  def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
    execWithJdbcConnection(execWithJdbcStatement(_, doCommit = true) { stmt =>
      if (logging) logger.info(s"execJdbcStatement: $sql")
      stmt.execute(sql)
    })
  }

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary function
   * @return row count for SQL Data Manipulation Language (DML) statements (see also Jdbc.Statement.execute())
   */
  def execJdbcDmlStatement(sql: String, logging: Boolean = true): Int = {
    execWithJdbcConnection(execWithJdbcStatement(_, doCommit = true) { stmt =>
      if (logging) logger.info(s"execJdbcDmlStatement: $sql")
      stmt.executeUpdate(sql)
    })
  }

  /**
   * Execute an SQL query and evaluate its ResultSet
   * @param sql sql query to execute
   * @param evalResultSet function to evaluate the JDBC ResultSet
   * @return the evaluated result
   */
  def execJdbcQuery[A](sql:String, evalResultSet: ResultSet => A ) : A = {
    execWithJdbcConnection(execWithJdbcStatement(_, doCommit = true) { stmt =>
      var rs: ResultSet = null
      try {
        logger.info(s"execJdbcQuery: $sql")
        rs = stmt.executeQuery(sql)
        evalResultSet(rs)
      } finally {
        if (rs != null) rs.close()
      }
    })
  }

  def test(): Unit = {
    execWithJdbcConnection{ _ => () }
  }

  def getAuthModeSparkOptions: Map[String,String] = {
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => Map( "user" -> m.userSecret.resolve(), "password" -> m.passwordSecret.resolve())
      case _ => throw new IllegalArgumentException(s"($id) ${authMode.getClass.getSimpleName} not supported.")
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

  /**
   * Begin database transaction. Note that depending on the isolation level of the database, changes from concurrent
   * connections might not be available inside the transaction once it is started. So make sure that any required writes
   * from Spark are finished before beginning a transaction.
   */
  def beginTransaction(): JdbcTransaction = {
    new JdbcTransaction(this)
  }

  /**
   * Class for handling database transactions. If all operations succeeded call [[commit]], otherwise [[rollback]].
   */
  class JdbcTransaction(connection: JdbcTableConnection) extends SmartDataLakeLogger {
    logger.info(s"($id) begin transaction")
    private val jdbcConnection: SqlConnection = connection.pool.borrowObject

    def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
      connection.execWithJdbcStatement(jdbcConnection, doCommit = false) { stmt =>
        if (logging) logger.info(s"($id) execJdbcStatement in transaction: $sql")
        if (autoCommit) logger.warn(s"($id) autoCommit is enabled, so statement will be committed immediately")
        stmt.execute(sql)
      }
    }

    def commit(): Unit = {
      if (!connection.autoCommit) {
        logger.info(s"($id) commit transaction")
        jdbcConnection.commit()
      };
      close()
    }

    def rollback(): Unit = {
      try {
        if (!connection.autoCommit) {
          logger.info(s"($id) roll back transaction")
          jdbcConnection.rollback()
        };
      } finally {
        close()
      }
    }

    private def close(): Unit = {
      connection.pool.returnObject(jdbcConnection)
    }

    private def id: Int = {
      hashCode()
    }
  }

  override def factory: FromConfigFactory[Connection] = JdbcTableConnection
}

object JdbcTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableConnection = {
    extract[JdbcTableConnection](config)
  }
}

/**
 * Helper methods to create and initialize JDBC connection
 *
 * Note: These methods are separated from JdbcTableConnection by intention to avoid using them JDBC connections without pool.
 * Jdbc connections should be created and managed by a pool.
 */
object JdbcConnectionFactory {
  def getConnection(jdbcTableConnection: JdbcTableConnection): SqlConnection = {
    // create connection
    Class.forName(jdbcTableConnection.driver)
    val connection = if (jdbcTableConnection.authMode.isDefined) jdbcTableConnection.authMode.get match {
      case m: BasicAuthMode => DriverManager.getConnection(jdbcTableConnection.url, m.userSecret.resolve(), m.passwordSecret.resolve())
      case _ => throw new IllegalArgumentException(s"${jdbcTableConnection.authMode.getClass.getSimpleName} not supported.")
    } else DriverManager.getConnection(jdbcTableConnection.url)

    // init connection
    jdbcTableConnection.connectionInitSql.foreach(initSql => {
      var stmt: Statement = null
      try {
        stmt = connection.createStatement()
        stmt.execute(initSql)
      } finally {
        if (stmt != null) stmt.close()
      }
    })
    connection.setAutoCommit(jdbcTableConnection.autoCommit)

    // return
    connection
  }
}