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

  /**
   * The definition the database would store for a view with the given query, or None if not supported.
   * Databases rewrite the query of a view, e.g. Postgres adds casts and removes aliases, so that the definition of
   * an existing view can only be compared with a query rewritten the same way.
   */
  def getViewDefinitionOfQuery(query: String): Option[String] = None

  /**
   * The definition of a materialized view as stored by the database, or None if it does not exist or the database
   * is not supported. Then the definition can not be compared, and the materialized view is replaced.
   */
  def getMaterializedViewDefinition(db: String, viewName: String): Option[String] = {
    materializedViewDefinitionQuery(removeQuotes(db).replace("'", "''"), removeQuotes(viewName).replace("'", "''"))
      .flatMap(query => connection.execJdbcQuery(query, (rs: ResultSet) => if (rs.next()) Option(rs.getString(1)) else None))
  }

  protected def materializedViewDefinitionQuery(db: String, viewName: String): Option[String] = None

  /**
   * The privileges granted on a table or view to other users or roles, or None if they can not be read for this
   * database. They are needed to grant them again when a materialized view is dropped and created again.
   * Privileges of the owner are not included, as the owner of the new object gets them anyway.
   */
  def getGrants(db: String, tableName: String): Option[Seq[TableGrant]] = {
    grantsQuery(removeQuotes(db).replace("'", "''"), removeQuotes(tableName).replace("'", "''"))
      .map(query => connection.execJdbcQuery(query, (rs: ResultSet) =>
        Iterator.continually(rs).takeWhile(_.next())
          .map(r => TableGrant(r.getString(1), r.getString(2), Option(r.getString(3)).exists(_.equalsIgnoreCase("YES"))))
          .toList
      ))
  }

  /**
   * Query returning the columns grantee, privilege and grantable ('YES' or 'NO') of the privileges on a table.
   */
  protected def grantsQuery(db: String, tableName: String): Option[String] = None

  /**
   * Create the statements granting the given privileges on a table or view.
   */
  def grantStatements(tableName: String, grants: Seq[TableGrant]): Seq[String] = grants.map { grant =>
    val grantee = if (grant.grantee.equalsIgnoreCase("PUBLIC")) "PUBLIC" else quoteIdentifier(grant.grantee)
    s"GRANT ${grant.privilege} ON $tableName TO $grantee${if (grant.grantable) " WITH GRANT OPTION" else ""}"
  }

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
      case d if d.toLowerCase.contains("postgresql") => new PostgresJdbcCatalog(connection, url)
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

  override protected def materializedViewDefinitionQuery(db: String, viewName: String): Option[String] =
    Some(s"SELECT QUERY FROM ALL_MVIEWS WHERE UPPER(OWNER) = UPPER('$db') AND UPPER(MVIEW_NAME) = UPPER('$viewName')")

  override protected def grantsQuery(db: String, tableName: String): Option[String] =
    Some(s"SELECT GRANTEE, PRIVILEGE, GRANTABLE FROM ALL_TAB_PRIVS WHERE UPPER(TABLE_SCHEMA) = UPPER('$db') AND UPPER(TABLE_NAME) = UPPER('$tableName')")
}

/**
 * PostgreSQL JDBC Catalog query implementation. Materialized views and their privileges are not listed in
 * INFORMATION_SCHEMA, so they are read from the system catalogs.
 */
class PostgresJdbcCatalog(connection: Connection with GenericJdbcExecution, url: String) extends DefaultJdbcCatalog(connection, url) {

  /**
   * Creates a temporary view with the query in a transaction which is rolled back, and reads its definition.
   * It is formatted like the definition of every view and materialized view, as all are created by pg_get_viewdef.
   */
  override def getViewDefinitionOfQuery(query: String): Option[String] = connection.execWithJdbcConnection { con =>
    val autoCommit = con.getAutoCommit
    con.setAutoCommit(false)
    val stmt = con.createStatement()
    try {
      stmt.execute(s"CREATE TEMPORARY VIEW sdlb_view_definition AS $query")
      val rs = stmt.executeQuery("SELECT pg_get_viewdef('pg_temp.sdlb_view_definition'::regclass)")
      if (rs.next()) Option(rs.getString(1)) else None
    } finally {
      stmt.close()
      con.rollback()
      con.setAutoCommit(autoCommit)
    }
  }

  override protected def materializedViewDefinitionQuery(db: String, viewName: String): Option[String] =
    Some(s"SELECT definition FROM pg_matviews WHERE UPPER(schemaname) = UPPER('$db') AND UPPER(matviewname) = UPPER('$viewName')")

  override protected def grantsQuery(db: String, tableName: String): Option[String] =
    Some(
      s"""SELECT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END, a.privilege_type,
         |  CASE WHEN a.is_grantable THEN 'YES' ELSE 'NO' END
         |FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace CROSS JOIN LATERAL aclexplode(c.relacl) a
         |WHERE UPPER(n.nspname) = UPPER('$db') AND UPPER(c.relname) = UPPER('$tableName') AND a.grantee <> c.relowner
         |ORDER BY 1, 2""".stripMargin)
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

/**
 * A privilege granted on a table or view, see [[JdbcCatalog.getGrants]].
 *
 * @param grantee the user or role, or PUBLIC
 * @param privilege e.g. SELECT
 * @param grantable true if the grantee may grant the privilege to others (WITH GRANT OPTION)
 */
case class TableGrant(grantee: String, privilege: String, grantable: Boolean)
