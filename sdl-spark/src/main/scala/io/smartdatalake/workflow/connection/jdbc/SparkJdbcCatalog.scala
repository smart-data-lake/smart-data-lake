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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.misc.{GenericJdbcExecution, SQLUtil}
import io.smartdatalake.workflow.connection.Connection
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * The parts of the [[JdbcCatalog]] depending on Spark: DDL statements for Spark data types, based on the Spark
 * JdbcDialect of the database.
 *
 * @param connection connection to execute statements
 * @param url JDBC url of the connection, used to determine the Spark JdbcDialect
 */
class SparkJdbcCatalog(connection: Connection with GenericJdbcExecution, url: String) {

  lazy val jdbcDialect: JdbcDialect = SparkJdbcCatalog.getJdbcDialect(url)

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
    sql.replace(jdbcDialect.quoteIdentifier(column), column)
  }

  // create ddl to add alter column type
  def getAlterColumnSql(table: String, column: String, sqlType: String): String = {
    val sql = jdbcDialect.getUpdateColumnTypeQuery(table, column, sqlType)
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(jdbcDialect.quoteIdentifier(column), column)
  }

  // create ddl to add alter column type
  def getAlterColumnNullableSql(table: String, column: String, isNullable: Boolean = true): String = {
    val sql = jdbcDialect.getUpdateColumnNullabilityQuery(table, column, isNullable)
    // we need to fix column name quotation as many dialects always quote them, which is not optimal.
    sql.replace(jdbcDialect.quoteIdentifier(column), column)
  }

  /**
   * Code partly copied from Spark: JdbcUtils to adapt schemaString method to not quote identifiers
   * if Spark is in case-insensitive mode.
   */
  def createTableFromSchema(tableName: String, schema: StructType, rawOptions: Map[String, String]): Unit = {
    def schemaString(
        schema: StructType,
        caseSensitive: Boolean,
        createTableColumnTypes: Option[String] = None
    ): String = {
      val sb = new StringBuilder()
      val userSpecifiedColTypesMap = createTableColumnTypes
        .map(parseUserSpecifiedCreateTableColumnTypes(caseSensitive, _))
        .getOrElse(Map.empty[String, String])
      schema.fields.foreach { field =>
        // Change is here - do not quote if not case-sensitive and normal characters used:
        val name = if (caseSensitive || SQLUtil.hasIdentifierSpecialChars(field.name)) jdbcDialect.quoteIdentifier(field.name)
        else field.name
        val typ = userSpecifiedColTypesMap
          .getOrElse(field.name, JdbcUtils.getJdbcType(field.dataType, jdbcDialect).databaseTypeDefinition)
        val nullable = if (field.nullable) "" else "NOT NULL"
        sb.append(s", $name $typ $nullable")
      }
      if (sb.length < 2) "" else sb.substring(2)
    }
    def parseUserSpecifiedCreateTableColumnTypes(caseSensitive: Boolean, createTableColumnTypes: String): Map[String, String] = {
      val userSchema = CatalystSqlParser.parseTableSchema(createTableColumnTypes)
      val userSchemaMap = userSchema.fields.map(f => f.name -> f.dataType.catalogString).toMap
      if (caseSensitive) userSchemaMap else CaseInsensitiveMap(userSchemaMap)
    }
    val options = new JdbcOptionsInWrite(url, tableName, rawOptions)
    val strSchema = schemaString(schema, Environment.caseSensitive, options.createTableColumnTypes)
    val createTableOptions = options.createTableOptions
    val sql = s"CREATE TABLE $tableName ($strSchema) $createTableOptions"
    connection.execJdbcStatement(sql)
  }
}

object SparkJdbcCatalog {
  private lazy val registerDialectsOnce: Unit = {
    JdbcDialects.registerDialect(HSQLDbDialect)
    JdbcDialects.registerDialect(MariaDbDialect)
  }

  /**
   * Register the custom Spark jdbc dialects of SDLB. This must be done before Spark reads or writes a JDBC table,
   * as Spark determines the dialect for creating tables by itself.
   */
  def registerDialects(): Unit = registerDialectsOnce

  /**
   * Get the Spark JdbcDialect for a JDBC url, including the custom dialects of SDLB.
   */
  def getJdbcDialect(url: String): JdbcDialect = {
    registerDialects()
    JdbcDialects.get(url)
  }
}
