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
import io.smartdatalake.definitions.{AuthMode, BasicAuthMode, Environment}
import io.smartdatalake.util.misc.SmartDataLakeLogger
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
 * @param metadata
 */
case class JdbcTableConnection( override val id: ConnectionId,
                                url: String,
                                driver: String,
                                authMode: Option[AuthMode] = None,
                                db: Option[String] = None,
                                override val metadata: Option[ConnectionMetadata] = None
                               ) extends Connection with SmartDataLakeLogger {

  // Allow only supported authentication modes
  private val supportedAuths = Seq(classOf[BasicAuthMode])
  require(authMode.isEmpty || supportedAuths.contains(authMode.get.getClass), s"${authMode.getClass.getSimpleName} not supported by ${this.getClass.getSimpleName}. Supported auth modes are ${supportedAuths.map(_.getSimpleName).mkString(", ")}.")

  // prepare catalog implementation
  val catalog: SQLCatalog = SQLCatalog.fromJdbcDriver(driver, this)

  def execJdbcStatement(sql:String, logging: Boolean = true) : Boolean = {
    var conn: SqlConnection = null
    var stmt: Statement = null
    try {
      conn = getConnection
      stmt = conn.createStatement
      if (logging) logger.info(s"execJdbcStatement: $sql")
      stmt.execute(sql)
    } finally {
      if (stmt!=null) stmt.close()
      if (conn!=null) conn.close()
    }
  }

  def execJdbcQuery[A](sql:String, evalResultSet: ResultSet => A ) : A = {
    var conn: SqlConnection = null
    var stmt: Statement = null
    var rs: ResultSet = null
    try {
      conn = getConnection
      stmt = conn.createStatement
      logger.info(s"execJdbcQuery: $sql")
      rs = stmt.executeQuery(sql)
      evalResultSet( rs )
    } finally {
      if (rs!=null) rs.close()
      if (stmt!=null) stmt.close()
      if (conn!=null) conn.close()
    }
  }

  def test(): Unit = {
    var conn: SqlConnection = null
    try {
      conn = getConnection
    } finally {
      if (conn!=null) conn.close()
    }
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

  override def factory: FromConfigFactory[Connection] = JdbcTableConnection
}

object JdbcTableConnection extends FromConfigFactory[Connection] {
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): JdbcTableConnection = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[JdbcTableConnection].value
  }
}


/**
 * SQL JDBC Catalog query method definition.
 * Implementations may vary depending on the concrete DB system.
 */
private[smartdatalake] abstract class SQLCatalog(connection: JdbcTableConnection) {
  // get spark jdbc dialect definitions
  protected val jdbcDialect: JdbcDialect = JdbcDialects.get(connection.url)
  def isDbExisting(db: String)(implicit session: SparkSession): Boolean
  def isTableExisting(db: String, table: String)(implicit session: SparkSession): Boolean
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
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where TABLE_SCHEMA='$db'"
      else
        s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where upper(TABLE_SCHEMA)=upper('$db')"
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists )
  }
  override def isTableExisting(db: String, table: String)(implicit session: SparkSession): Boolean = {
    val dbPrefix = if(db.equals("")) "" else db+"."
    val existsQuery = jdbcDialect.getTableExistsQuery(dbPrefix+table)
    connection.execJdbcStatement(existsQuery)
  }
}

/**
 * Oracle SQL JDBC Catalog query implementation
 */
private[smartdatalake] class OracleSQLCatalog(connection: JdbcTableConnection) extends SQLCatalog(connection) {
  override def isDbExisting(db: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from ALL_USERS where USERNAME='$db'"
      else
        s"select count(*) from ALL_USERS where upper(USERNAME)=upper('$db')"
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists)
  }
  override def isTableExisting(db: String, table: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from ((select TABLE_NAME as name from ALL_TABLES where TABLE_NAME='$table' and OWNER='$db') union all (select VIEW_NAME as name from ALL_VIEWS where VIEW_NAME='$table' and OWNER='$db'))"
      else
        s"select count(*) from ((select TABLE_NAME as name from ALL_TABLES where TABLE_NAME=upper('$table') and OWNER=upper('$db')) union all (select VIEW_NAME as name from ALL_VIEWS where VIEW_NAME=upper('$table') and OWNER=upper('$db')))"
    connection.execJdbcQuery( cntTableInCatalog, evalRecordExists )
  }
}

/**
 * SAP HANA JDBC Catalog query implementation
 */
private[smartdatalake] class SapHanaSQLCatalog(connection: JdbcTableConnection) extends SQLCatalog(connection) {
  override def isDbExisting(db: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from PUBLIC.SCHEMAS where SCHEMA_NAME='$db'"
      else
        s"select count(*) from PUBLIC.SCHEMAS where upper(SCHEMA_NAME)=upper('$db')"
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists)
  }
  override def isTableExisting(db: String, table: String)(implicit session: SparkSession): Boolean = {
    val cntTableInCatalog =
      if (Environment.enableJdbcCaseSensitivity)
        s"select count(*) from ((select TABLE_NAME as name from PUBLIC.TABLES where TABLE_NAME='$table' and SCHEMA_NAME='$db') union all (select VIEW_NAME as name from PUBLIC.VIEWS where VIEW_NAME='$table' and SCHEMA_NAME='$db'))"
      else
        s"select count(*) from ((select TABLE_NAME as name from PUBLIC.TABLES where upper(TABLE_NAME)=upper('$table') and upper(SCHEMA_NAME)=upper('$db')) union all (select VIEW_NAME as name from PUBLIC.VIEWS where upper(VIEW_NAME)=upper('$table') and upper(SCHEMA_NAME)=upper('$db')))"
    connection.execJdbcQuery( cntTableInCatalog, evalRecordExists )
  }
}
