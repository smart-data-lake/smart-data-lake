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
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode}
import io.smartdatalake.util.misc.{SQLUtil, SchemaUtil, SmartDataLakeLogger, TryWithResourcePool}
import io.smartdatalake.workflow.connection.{Connection, ConnectionMetadata}
import io.smartdatalake.workflow.dataobject.JdbcTableDataObject
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getJdbcType
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.StructType

import java.sql.{DriverManager, ResultSet, Statement, Connection => SqlConnection}
import java.time.Duration

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
 */
case class JdbcTableConnection(override val id: ConnectionId,
                               url: String,
                               driver: String,
                               authMode: Option[AuthMode] = None,
                               db: Option[String] = None,
                               maxParallelConnections: Int = 3,
                               connectionPoolMaxIdleTimeSec: Int = 3,
                               connectionPoolMaxWaitTimeSec: Int = 600,
                               override val metadata: Option[ConnectionMetadata] = None,
                               @Deprecated @deprecated("Enabling autoCommit is no longer recommended.", "2.5.0") autoCommit: Boolean = false,
                               connectionInitSql: Option[String] = None
                               ) extends Connection with SmartDataLakeLogger {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(authMode.isEmpty || supportedAuths.contains(authMode.get.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  // prepare catalog implementation
  val catalog: JdbcCatalog = JdbcCatalog.fromJdbcDriver(driver, this)

  /**
   * Get a connection from the pool and execute an arbitrary function
   */
  def execWithJdbcConnection[A]( func: SqlConnection => A ): A = {
    TryWithResourcePool.exec(pool){
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
    } finally {
      if (stmt != null) stmt.close()
    }
  }

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary function
   */
  def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
    execWithJdbcConnection(execWithJdbcStatement(_, doCommit = true) { stmt =>
      if (logging) logger.info(s"execJdbcStatement: $sql")
      stmt.execute(sql)
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
    execWithJdbcConnection{ _ => Unit }
  }

  private def getConnection: SqlConnection = {
    Class.forName(driver)
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => DriverManager.getConnection(url, m.user, m.password)
      case _ => throw new IllegalArgumentException(s"${authMode.getClass.getSimpleName} not supported.")
    } else DriverManager.getConnection(url)
  }

  def getAuthModeSparkOptions: Map[String,String] = {
    if (authMode.isDefined) authMode.get match {
      case m: BasicAuthMode => Map( "user" -> m.user, "password" -> m.password )
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
    val strSchema = schemaString(schema, SchemaUtil.isSparkCaseSensitive, options.url, options.createTableColumnTypes)
    val createTableOptions = options.createTableOptions
    val sql = s"CREATE TABLE $tableName ($strSchema) $createTableOptions"
    execJdbcStatement(sql)
  }

  def dropTable(tableName: String, logging: Boolean = true): Unit = {
    if (catalog.isTableExisting(tableName)) {
      execJdbcStatement(s"drop table $tableName", logging = logging)
    }
  }

  // setup connection pool
  val pool = new GenericObjectPool[SqlConnection](new JdbcClientPoolFactory)
  pool.setMaxTotal(maxParallelConnections)
  pool.setMinEvictableIdle(Duration.ofSeconds(connectionPoolMaxIdleTimeSec)) // timeout to close jdbc connection if not in use
  pool.setMaxWait(Duration.ofSeconds(connectionPoolMaxWaitTimeSec))

  private class JdbcClientPoolFactory extends BasePooledObjectFactory[SqlConnection] {
    override def create(): SqlConnection = {
      val connection = getConnection
      initConnection(connection)
    }

    private def initConnection(connection: SqlConnection): SqlConnection = {
      connectionInitSql.foreach(initSql => {
        var stmt: Statement = null
        try {
          stmt = connection.createStatement()
          stmt.execute(initSql)
        } finally {
          if (stmt != null) stmt.close()
        }
      })
      connection.setAutoCommit(autoCommit)
      connection
    }

    override def wrap(con: SqlConnection): PooledObject[SqlConnection] = new DefaultPooledObject(con)
    override def destroyObject(p: PooledObject[SqlConnection]): Unit = p.getObject.close()
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
    private val jdbcConnection: SqlConnection = connection.pool.borrowObject()

    def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
      connection.execWithJdbcStatement(jdbcConnection, doCommit = false) { stmt =>
        if (logging) logger.info(s"($id) execJdbcStatement in transaction: $sql")
        if (autoCommit) logger.warn("autoCommit is enabled, so statement will be committed immediately")
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