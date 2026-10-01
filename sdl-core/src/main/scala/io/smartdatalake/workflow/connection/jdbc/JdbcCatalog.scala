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

import io.smartdatalake.util.misc.{GenericJdbcExecution, SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.connection.Connection
import io.smartdatalake.workflow.dataobject.generic.{ForeignKeyDefinition, PrimaryKeyDefinition}

import scala.collection.mutable.{Set => MutableSet}
import java.sql.{ResultSet, SQLException}

/**
 * SQL JDBC Catalog query method definition.
 * Implementations may vary depending on the concrete DB system.
 *
 * Note that the catalog is independent of any DataFrame engine. DDL statements depending on the data types of an
 * engine, e.g. for schema evolution, are created by the engine, see `SparkJdbcCatalog` in sdl-spark.
 *
 * @param connection connection to execute catalog queries
 * @param url JDBC url of the connection, used to determine how identifiers are quoted
 */
abstract class JdbcCatalog(connection: Connection with GenericJdbcExecution, url: String) extends SmartDataLakeLogger  {

  // identifiers are quoted with double quotes (standard SQL), except by databases in MySQL tradition
  protected lazy val (quoteStart, quoteEnd) = {
    val subProtocol = url.split(':').drop(1).headOption.map(_.toLowerCase).getOrElse("")
    if (JdbcCatalog.backtickQuotingSubProtocols.contains(subProtocol)) ("`", "`") else ("\"", "\"")
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
    s"$quoteStart${s.replace(quoteEnd, quoteEnd + quoteEnd)}$quoteEnd"
  }

  // create ddl to set the comment of a table.
  // "COMMENT ON" is standard SQL and supported by most databases, but e.g. not by MySQL and MS SQL Server.
  def getCommentOnTableSql(tableName: String, comment: String): String =
    s"COMMENT ON TABLE $tableName IS '${SQLUtil.escapeSqlStringLiteral(comment)}'"

  // create ddl to set the comment of a column
  def getCommentOnColumnSql(tableName: String, column: String, comment: String): String =
    s"COMMENT ON COLUMN $tableName.$column IS '${SQLUtil.escapeSqlStringLiteral(comment)}'"

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

  def createForeignKeyConstraint(tableName: String, foreignKey: ForeignKeyDefinition, quoteCaseSensitiveColumn: String => String, logging: Boolean = true): Unit = {
    connection.execJdbcStatement(SQLUtil.createForeignKeyStatement(tableName, foreignKey, quoteCaseSensitiveColumn), logging = logging)
  }

  def dropForeignKeyConstraint(tableName: String, constraintName: String, logging: Boolean = true): Unit = {
    connection.execJdbcStatement(f"ALTER TABLE $tableName DROP CONSTRAINT $constraintName", logging = logging)
  }

  def isTableExisting(tableName: String): Boolean = {
    val tableExistsQuery = s"SELECT 1 FROM $tableName WHERE 1=0"
    try {
      connection.execJdbcStatement(tableExistsQuery, logging = false)
      true
    } catch {
      case _: Throwable =>
        logger.debug("No access on table or table does not exist: " +tableName)
        false
    }
  }

  /**
   * The definition of a view as stored by the database, or None if the view does not exist.
   * Depending on the database this is the query of the view, or the whole `CREATE VIEW` statement, e.g. for DuckDB.
   * The default implementation reads INFORMATION_SCHEMA.VIEWS. Identifiers are compared case-insensitively.
   */
  def getViewDefinition(db: String, viewName: String): Option[String] = {
    viewDefinitionQuery(removeQuotes(db).replace("'", "''"), removeQuotes(viewName).replace("'", "''"))
      .flatMap(query => connection.execJdbcQuery(query, (rs: ResultSet) => if (rs.next()) Option(rs.getString(1)) else None))
  }

  protected def viewDefinitionQuery(db: String, viewName: String): Option[String] =
    Some(s"SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE UPPER(TABLE_SCHEMA) = UPPER('$db') AND UPPER(TABLE_NAME) = UPPER('$viewName')")

  protected def evalRecordExists( rs:ResultSet ) : Boolean = {
    rs.next
    rs.getInt(1) == 1
  }

  /**
   * Convert the result of [[java.sql.DatabaseMetaData.getImportedKeys]] into foreign key definitions.
   * Note that the referenced table is qualified with its schema, but not with its catalog, as
   * JdbcTableDataObject does not use a catalog.
   */
  def handleForeignKeyResultSet(resultSet: ResultSet): Seq[ForeignKeyDefinition] = {
    var rows: Seq[(Option[String], String, String, String)] = Seq()
    while (resultSet.next()) {
      val referencedTable = Seq(Option(resultSet.getString("PKTABLE_SCHEM")), Option(resultSet.getString("PKTABLE_NAME")))
        .flatten.mkString(".")
      rows = rows :+ (Option(resultSet.getString("FK_NAME")), resultSet.getString("FKCOLUMN_NAME"),
        resultSet.getString("PKCOLUMN_NAME"), referencedTable)
    }
    rows.groupBy { case (fkName, _, _, referencedTable) => (fkName, referencedTable) }.toSeq
      .map { case ((fkName, referencedTable), constraintRows) =>
        ForeignKeyDefinition(constraintRows.map { case (_, fkColumn, pkColumn, _) => fkColumn -> pkColumn }.toMap,
          referencedTable, fkName)
      }
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
}
object JdbcCatalog {
  // JDBC sub protocols of databases quoting identifiers with backticks
  private val backtickQuotingSubProtocols = Set("mysql", "mariadb", "databricks")

  def fromJdbcDriver(driver: String, connection: Connection with GenericJdbcExecution, url: String): JdbcCatalog = {
    driver match {
      case d if d.toLowerCase.contains("oracle") => new OracleJdbcCatalog(connection, url)
      case d if d.toLowerCase.contains("com.sap.db") => new SapHanaJdbcCatalog(connection, url)
      case _ => new DefaultJdbcCatalog(connection, url)
    }
  }
}

/**
 * Default SQL JDBC Catalog query implementation using INFORMATION_SCHEMA
 */
class DefaultJdbcCatalog(connection: Connection with GenericJdbcExecution, url: String) extends JdbcCatalog(connection, url) {
  override def isDbExisting(db: String): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db)) {
      s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where TABLE_SCHEMA='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from INFORMATION_SCHEMA.SCHEMATA where UPPER(TABLE_SCHEMA)=UPPER('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists )
  }

  //This method is not used in JdbcTableDataObject, but in other DataObjects.
  // For this reason, it is not implemented in Oracle and SAP-HANA.
  def getPrimaryKey(catalog: Option[String], schema: Option[String], tableName: String) = {
    val catalogConstraint = if (catalog.isEmpty) "" else f" and TABLE_CATALOG = '${catalog.get}'"
    val schemaConstraint =  if (schema.isEmpty) "" else f" and TABLE_SCHEMA = '${schema.get}'"
    val baseQuery = f"select COLUMN_NAME, CONSTRAINT_NAME as PK_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_NAME = '$tableName'"
    val query = Seq(baseQuery, schemaConstraint, catalogConstraint).mkString
    connection.execJdbcQuery(query, handlePrimaryKeyResultSet)
  }
}

/**
 * Oracle SQL JDBC Catalog query implementation
 */
class OracleJdbcCatalog(connection: Connection with GenericJdbcExecution, url: String) extends JdbcCatalog(connection, url) {
  override def isDbExisting(db: String): Boolean = {
    val cntTableInCatalog = if(isQuotedIdentifier(db))  {
      s"select count(*) from ALL_USERS where USERNAME='${removeQuotes(db)}'"
    }
    else {
      s"select count(*) from ALL_USERS where UPPER(USERNAME)=UPPER('$db')"
    }
    connection.execJdbcQuery(cntTableInCatalog, evalRecordExists)
  }

  override protected def viewDefinitionQuery(db: String, viewName: String): Option[String] =
    Some(s"SELECT TEXT FROM ALL_VIEWS WHERE UPPER(OWNER) = UPPER('$db') AND UPPER(VIEW_NAME) = UPPER('$viewName')")
}

/**
 * SAP HANA JDBC Catalog query implementation
 */
class SapHanaJdbcCatalog(connection: Connection with GenericJdbcExecution, url: String) extends JdbcCatalog(connection, url) {
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
