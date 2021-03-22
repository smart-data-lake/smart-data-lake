/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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

import java.sql.{DriverManager, ResultSet, Statement, Connection => SqlConnection}

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode}
import io.smartdatalake.util.misc.{SmartDataLakeLogger, TryWithResourcePool}
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}

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
 * @param metadata
 */
case class JdbcTableConnection( override val id: ConnectionId,
                                url: String,
                                driver: String,
                                authMode: Option[AuthMode] = None,
                                db: Option[String] = None,
                                maxParallelConnections: Int = 1,
                                override val metadata: Option[ConnectionMetadata] = None
                               ) extends Connection with SmartDataLakeLogger {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(authMode.isEmpty || supportedAuths.contains(authMode.get.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  // prepare catalog implementation
  val catalog: SQLCatalog = SQLCatalog.fromJdbcDriver(driver, this)

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
  def execWithJdbcStatement[A]( func: Statement => A ): A = {
    execWithJdbcConnection { conn =>
      var stmt: Statement = null
      try {
        stmt = conn.createStatement
        func(stmt)
      } finally {
        if (stmt != null) stmt.close()
      }
    }
  }

  /**
   * Execute an SQL statement
   * @return true if the first result is a ResultSet object; false if it is an update count or there are no results
   */
  def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
    execWithJdbcStatement { stmt =>
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
    execWithJdbcStatement { stmt =>
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

  // setup connection pool
  val pool = new GenericObjectPool[SqlConnection](new JdbcClientPoolFactory)
  pool.setMaxTotal(maxParallelConnections)
  pool.setMaxIdle(1) // keep max one idle jdbc connection
  pool.setMinEvictableIdleTimeMillis(3000) // timeout to close jdbc connection if not in use
  private class JdbcClientPoolFactory extends BasePooledObjectFactory[SqlConnection] {
    override def create(): SqlConnection = getConnection
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


/**
 * SQL JDBC Catalog query method definition.
 * Implementations may vary depending on the concrete DB system.
 */
private[smartdatalake] abstract class SQLCatalog(connection: JdbcTableConnection) extends SmartDataLakeLogger  {
  // get spark jdbc dialect definitions
  protected val jdbcDialect: JdbcDialect = JdbcDialects.get(connection.url)
  protected val isNoopDialect: Boolean = jdbcDialect.getClass.getSimpleName.startsWith("NoopDialect") // The default implementation is used for unknown url types
  // use jdbcDialect to define identifiers used for quoting
  protected val (quoteStart,quoteEnd) = {
    val dbUnquoted = jdbcDialect.quoteIdentifier("dummy")
    val quoteStart = dbUnquoted.replaceFirst("dummy.*", "")
    val quoteEnd = dbUnquoted.replaceFirst(".*dummy", "")
    (quoteStart, quoteEnd)
  }
  // true if the given identifier is quoted
  def isQuotedIdentifier(s: String) : Boolean = {
    s.startsWith(quoteStart) && s.endsWith(quoteEnd)
  }
  // returns the string with quotes removed
  def removeQuotes(s: String) : String = {
    s.stripPrefix(quoteStart).stripSuffix(quoteEnd)
  }

  def isDbExisting(db: String)(implicit session: SparkSession): Boolean
  def isTableExisting(db: String, table: String)(implicit session: SparkSession): Boolean = {
    val dbPrefix = if (db.equals("")) "" else db + "."
    val tableExistsQuery = jdbcDialect.getTableExistsQuery(dbPrefix+table)
    try {
      connection.execJdbcStatement(tableExistsQuery, logging = false)
      true
    }
    catch {
      case _: Throwable => {
        logger.info("No access on table or table does not exist: " +dbPrefix+table)
        false
      }
    }
  }

  protected def evalRecordExists( rs:ResultSet ) : Boolean = {
    rs.next
    rs.getInt(1) == 1
  }
}
private[smartdatalake] object SQLCatalog {
  def fromJdbcDriver(driver: String, connection: JdbcTableConnection): SQLCatalog = {
    driver match {
      case d if d.toLowerCase.contains("oracle") => new OracleSQLCatalog(connection)
      case d if d.toLowerCase.contains("com.sap.db") => new SapHanaSQLCatalog(connection)
      case _ => new DefaultSQLCatalog(connection)
    }
  }
}

/**
 * Default SQL JDBC Catalog query implementation using INFORMATION_SCHEMA
 */
private[smartdatalake] class DefaultSQLCatalog(connection: JdbcTableConnection) extends SQLCatalog(connection) {
  override def isDbExisting(db: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db)) {
      s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where TABLE_SCHEMA='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where UPPER(TABLE_SCHEMA)=UPPER('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists )
  }
}

/**
 * Oracle SQL JDBC Catalog query implementation
 */
private[smartdatalake] class OracleSQLCatalog(connection: JdbcTableConnection) extends SQLCatalog(connection) {
  override def isDbExisting(db: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db))  {
      s"select count(*) from ALL_USERS where USERNAME='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from ALL_USERS where UPPER(USERNAME)=UPPER('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists)
  }
}

/**
 * SAP HANA JDBC Catalog query implementation
 */
private[smartdatalake] class SapHanaSQLCatalog(connection: JdbcTableConnection) extends SQLCatalog(connection) {
  override def isDbExisting(db: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db))  {
      s"select count(*) from PUBLIC.SCHEMAS where SCHEMA_NAME='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from PUBLIC.SCHEMAS where upper(SCHEMA_NAME)=upper('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists)
  }
}
