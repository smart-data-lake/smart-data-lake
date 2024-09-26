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

import io.smartdatalake.util.misc.{JdbcExecution, SmartDataLakeLogger}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.PrimaryKeyDefinition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.{Set => MutableSet}
import java.sql.{ResultSet, SQLException, Connection => SqlConnection}

/**
 * SQL JDBC Catalog query method definition.
 * Implementations may vary depending on the concrete DB system.
 */
private[smartdatalake] abstract class JdbcCatalog(connection: Connection with JdbcExecution) extends SmartDataLakeLogger  {
  // get spark jdbc dialect definitions
  JdbcDialects.registerDialect(HSQLDbDialect)
  JdbcDialects.registerDialect(MariaDbDialect)
  protected lazy val jdbcDialect: JdbcDialect = connection.jdbcDialect
  protected lazy val isNoopDialect: Boolean = jdbcDialect.getClass.getSimpleName.startsWith("NoopDialect") // The default implementation is used for unknown url types
  // use jdbcDialect to define identifiers used for quoting
  protected lazy val (quoteStart,quoteEnd) = {
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
    val sqlType = JdbcUtils.getJdbcType(t, jdbcDialect)
    val nullable = if (!isNullable) " NOT NULL" else ""
    s"${sqlType.databaseTypeDefinition}$nullable"
  }

  // create ddl to add a column
  def getAddColumnSql(table: String, column: String, dataType: String): String = {
    val sql = jdbcDialect.getAddColumnQuery(table, column, dataType)
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(quoteIdentifier(column), column)
  }

  // create ddl to add alter column type
  def getAlterColumnSql(table: String, column: String, sqlType: String): String = {
    val sql = jdbcDialect.getUpdateColumnTypeQuery(table, column, sqlType)
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(quoteIdentifier(column), column)
  }

  // create ddl to add alter column type
  def getAlterColumnNullableSql(table: String, column: String, isNullable: Boolean = true): String = {
    val sql = jdbcDialect.getUpdateColumnNullabilityQuery(table, column, isNullable)
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(quoteIdentifier(column), column)
  }

  def isDbExisting(db: String): Boolean

  def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String], logging: Boolean = true): Unit = {
    if (isTableExisting(tableName)) {
      val stmt: String = f"ALTER TABLE $tableName ADD CONSTRAINT $constraintName PRIMARY KEY (${cols.mkString(",")})"
      connection.execJdbcStatement(stmt, logging = logging)
    }
  }

  def dropPrimaryKeyConstraint(tableName: String, constraintName: String, logging: Boolean = true): Unit = {
    if (isTableExisting(tableName)) {
      val stmt: String = f"ALTER TABLE $tableName DROP CONSTRAINT $constraintName"
      connection.execJdbcStatement(stmt, logging = logging)
    }
  }

  def isTableExisting(tableName: String): Boolean = {
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

  def getSchemaFromTable(table: String): StructType = {
    val schemaQuery = jdbcDialect.getSchemaQuery(table)
    connection.execJdbcQuery(schemaQuery, JdbcUtils.getSchema(_, jdbcDialect))
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
private[smartdatalake] class DefaultJdbcCatalog(connection: Connection with JdbcExecution) extends JdbcCatalog(connection) {
  override def isDbExisting(db: String): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db)) {
      s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where TABLE_SCHEMA='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where UPPER(TABLE_SCHEMA)=UPPER('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists )
  }

  def handlePrimaryKeyResultSet(resultSet: ResultSet): Option[PrimaryKeyDefinition] = {
    var primaryKeyCols: MutableSet[String] = MutableSet()
    var primaryKeyName: MutableSet[String] = MutableSet()
    while (resultSet.next()) {
      primaryKeyCols += resultSet.getString("COLUMN_NAME")
      primaryKeyName += resultSet.getString("PK_NAME")
    }
    (primaryKeyCols.toList, primaryKeyName.toList) match {
      case (List(), _) => None
      case (cols, List()) => Some(PrimaryKeyDefinition(cols))
      case (_, pk) if pk.size > 1 => throw new SQLException(f"The JDBC-Connection more than one Primary Key!")
      case (cols, pk) => Some(PrimaryKeyDefinition(cols, Some(pk.head)))
    }
  }

  //This method is not used in JdbcTableDataObject, but in other DataObjects.
  // For this reason, it is not implemented in Oracle and SAP-HANA.
  def getPrimaryKey(catalog: String, schema: String, tableName: String) = {
    val catalogConstraint = if (catalog.isEmpty) "" else f" and TABLE_CATALOG = '$catalog'"
    val schemaConstraint =  if (schema.isEmpty) "" else f" and TABLE_SCHEMA = '$schema'"
    val baseQuery = f"select COLUMN_NAME, CONSTRAINT_NAME as PK_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_NAME = '$tableName'"
    val query = Seq(baseQuery, schemaConstraint, catalogConstraint).mkString
    connection.execJdbcQuery(query, handlePrimaryKeyResultSet)
  }
}

/**
 * Oracle SQL JDBC Catalog query implementation
 */
private[smartdatalake] class OracleJdbcCatalog(connection: Connection with JdbcExecution) extends JdbcCatalog(connection) {
  override def isDbExisting(db: String): Boolean = {
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
private[smartdatalake] class SapHanaJdbcCatalog(connection: Connection with JdbcExecution) extends JdbcCatalog(connection) {
  override def isDbExisting(db: String): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db))  {
      s"select count(*) from PUBLIC.SCHEMAS where SCHEMA_NAME='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from PUBLIC.SCHEMAS where upper(SCHEMA_NAME)=upper('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists)
  }
}
