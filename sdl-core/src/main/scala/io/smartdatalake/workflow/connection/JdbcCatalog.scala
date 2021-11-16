/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import io.smartdatalake.util.misc.SmartDataLakeLogger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getCommonJDBCType
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.DataType

import java.sql.ResultSet

/**
 * SQL JDBC Catalog query method definition.
 * Implementations may vary depending on the concrete DB system.
 */
private[smartdatalake] abstract class JdbcCatalog(connection: JdbcTableConnection) extends SmartDataLakeLogger  {
  // get spark jdbc dialect definitions
  JdbcDialects.registerDialect(HSQLDbDialect)
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
  // quote identifier for this database
  def quoteIdentifier(s: String) : String = {
    jdbcDialect.quoteIdentifier(s)
  }

  // convert Spark DataType to SQL type
  def getSqlType(t: DataType, isNullable: Boolean = true): String = {
    val sqlType = jdbcDialect.getJDBCType(t).orElse(getCommonJDBCType(t)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${t.catalogString}"))
    val nullable = if (!isNullable) " NOT NULL" else ""
    s"${sqlType.databaseTypeDefinition}$nullable"
  }

  // create ddl to add a column
  def getAddColumnSql(table: String, column: String, dataType: String): String = {
    val sql = s"ALTER TABLE $table ADD $column $dataType"
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(quoteIdentifier(column), column)
  }

  // create ddl to add alter column type
  def getAlterColumnSql(table: String, column: String, sqlType: String): String = {
    val sql = s"ALTER TABLE $table MODIFY $column $sqlType"
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(quoteIdentifier(column), column)
  }

  // create ddl to add alter column type
  def getAlterColumnNullableSql(table: String, column: String, isNullable: Boolean = true): String = {
    val sql = s"ALTER TABLE $table ALTER $column SET ${if (isNullable) "NULL" else "NOT NULL"}"
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(quoteIdentifier(column), column)
  }

  def isDbExisting(db: String)(implicit session: SparkSession): Boolean
  def isTableExisting(tableName: String)(implicit session: SparkSession): Boolean = {
    val tableExistsQuery = jdbcDialect.getTableExistsQuery(tableName)
    try {
      connection.execJdbcStatement(tableExistsQuery, logging = false)
      true
    } catch {
      case _: Throwable =>
        logger.debug("No access on table or table does not exist: " +tableName)
        false
    }
  }

  protected def evalRecordExists( rs:ResultSet ) : Boolean = {
    rs.next
    rs.getInt(1) == 1
  }
}
private[smartdatalake] object JdbcCatalog {
  def fromJdbcDriver(driver: String, connection: JdbcTableConnection): JdbcCatalog = {
    driver match {
      case d if d.toLowerCase.contains("oracle") => new OracleJdbcCatalog(connection)
      case d if d.toLowerCase.contains("com.sap.db") => new SapHanaJdbcCatalog(connection)
      case _ => new DefaultJdbcCatalog(connection)
    }
  }
}

/**
 * Default SQL JDBC Catalog query implementation using INFORMATION_SCHEMA
 */
private[smartdatalake] class DefaultJdbcCatalog(connection: JdbcTableConnection) extends JdbcCatalog(connection) {
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
private[smartdatalake] class OracleJdbcCatalog(connection: JdbcTableConnection) extends JdbcCatalog(connection) {
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
private[smartdatalake] class SapHanaJdbcCatalog(connection: JdbcTableConnection) extends JdbcCatalog(connection) {
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
