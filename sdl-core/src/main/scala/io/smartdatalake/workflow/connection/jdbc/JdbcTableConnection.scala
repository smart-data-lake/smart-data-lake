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
import io.smartdatalake.util.misc.{SchemaUtil, SmartDataLakeLogger, TryWithResourcePool}
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

/**
 * Connection information for jdbc tables.
 * If authentication is needed, user and password must be provided.
 *
 * @param id unique id of this connection
 * @param url jdbc connection url
 * @param driver class name of jdbc driver
 * @param authMode optional authentication information: for now BasicAuthMode is supported.
 * @param db jdbc database
 * @param maxParallelConnections number of parallel jdbc connections created by an instance of this connection
 *                               Note that Spark manages JDBC Connections on its own. This setting only applies to JDBC connection
 *                               used by SDL for validating metadata or pre/postSQL.
 * @param connectionPoolMaxIdleTimeSec timeout to close unused connections in the pool
 * @param enableCommit whether to enable commits for transactional behaviour in SDL
 *                     Note that most JDBC drivers use auto-commit per default which is not recommended when [[enableCommit]] is true.
 *                     To disable auto-commit see [[autoCommit]].
 * @param autoCommit flag to enable or disable the auto-commit behaviour of the JDBC driver
 *                   If not set, the default auto-commit mode of the JDBC driver is used.
 * @param connectionInitSql SQL statement to be executed every time a new connection is created, for example to set session parameters
 */
case class JdbcTableConnection(override val id: ConnectionId,
                               url: String,
                               driver: String,
                               authMode: Option[AuthMode] = None,
                               db: Option[String] = None,
                               maxParallelConnections: Int = 1,
                               connectionPoolMaxIdleTimeSec: Int = 3,
                               override val metadata: Option[ConnectionMetadata] = None,
                               enableCommit: Boolean = true,
                               autoCommit: Option[Boolean] = None,
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

  /**
   * Get a JDBC connection from the pool, create a JDBC statement and execute an arbitrary function
   */
  def execWithJdbcStatement[A](doCommit: Boolean = false)(func: Statement => A ): A = {
    execWithJdbcConnection { conn =>
      var stmt: Statement = null
      try {
        stmt = conn.createStatement
        val result = func(stmt)
        if (doCommit && enableCommit) conn.commit()
        result
      } finally {
        if (stmt != null) stmt.close()
      }
    }
  }

  /**
   * Execute an SQL statement
   * @return true if the first result is a ResultSet object; false if it is an update count or there are no results
   */
  def execJdbcStatement(sql:String, logging: Boolean = true, doCommit: Boolean = false) : Boolean = {
    execWithJdbcStatement(doCommit) { stmt =>
      if (logging) logger.info(s"execJdbcStatement: $sql")
      stmt.execute(sql)
    }
  }

  /**
   * Execute an SQL query and evaluate its ResultSet
   * @param sql sql query to execute
   * @param evalResultSet function to evaluate the JDBC ResultSet
   * @return the evaluated result
   */
  def execJdbcQuery[A](sql:String, evalResultSet: ResultSet => A ) : A = {
    execWithJdbcStatement() { stmt =>
      var rs: ResultSet = null
      try {
        logger.info(s"execJdbcQuery: $sql")
        rs = stmt.executeQuery(sql)
        evalResultSet(rs)
      } finally {
        if (rs != null) rs.close()
      }
    }
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
        val name = if(caseSensitive || JdbcTableDataObject.hasIdentifierSpecialChars(field.name)) dialect.quoteIdentifier(field.name)
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
  pool.setMaxIdle(1) // keep max one idle jdbc connection
  pool.setMinEvictableIdleTimeMillis(connectionPoolMaxIdleTimeSec * 1000) // timeout to close jdbc connection if not in use
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
      autoCommit.foreach(connection.setAutoCommit)
      connection
    }

    override def wrap(con: SqlConnection): PooledObject[SqlConnection] = new DefaultPooledObject(con)
    override def destroyObject(p: PooledObject[SqlConnection]): Unit = p.getObject.close()
  }

  override def factory: FromConfigFactory[Connection] = JdbcTableConnection
}

object JdbcTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): JdbcTableConnection = {
    extract[JdbcTableConnection](config)
  }
}