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

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, GeneralAggregateFunc}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._

import java.sql.{SQLFeatureNotSupportedException, Types}
import java.util.Locale
import scala.collection.mutable

/**
 * Spark does not provide a [[JdbcDialect]] for MariaDb, so we provide one here.
 * The [[MariaDbDialect]] is based on [[org.apache.spark.sql.jdbc.MySQLDialect]].
 */
private case object MariaDbDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.toLowerCase(Locale.ROOT).startsWith("jdbc:mariadb")

  // See https://mariadb.com/kb/en/aggregate-functions/
  override def compileAggregate(aggFunction: AggregateFunc): Option[String] = {
    super.compileAggregate(aggFunction).orElse(
      aggFunction match {
        case f: GeneralAggregateFunc if f.name() == "VAR_POP" && !f.isDistinct =>
          assert(f.children().length == 1)
          Some(s"VAR_POP(${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "VAR_SAMP" && !f.isDistinct =>
          assert(f.children().length == 1)
          Some(s"VAR_SAMP(${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_POP" && !f.isDistinct =>
          assert(f.children().length == 1)
          Some(s"STDDEV_POP(${f.children().head})")
        case f: GeneralAggregateFunc if f.name() == "STDDEV_SAMP" && !f.isDistinct =>
          assert(f.children().length == 1)
          Some(s"STDDEV_SAMP(${f.children().head})")
        case _ => None
      }
    )
  }

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else None
  }

  override def quoteIdentifier(colName: String): String = {
    s"`${colName}`"
  }

  override def schemasExists(conn: java.sql.Connection, options: JDBCOptions, schema: String): Boolean = {
    listSchemas(conn, options).exists(_.head == schema)
  }

  override def listSchemas(connection: java.sql.Connection, options: JDBCOptions): Array[Array[String]] = {
    val schemaBuilder = mutable.ArrayBuilder.make[Array[String]]
    try {
      JdbcUtils.executeQuery(connection, options, "SHOW SCHEMAS") { rs =>
        while (rs.next()) {
          schemaBuilder += Array(rs.getString("Database"))
        }
      }
    } catch {
      case _: Exception =>
        logWarning("Cannot show schemas.")
    }
    schemaBuilder.result()
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)

  // See https://mariadb.com/kb/en/alter-table/
  override def getUpdateColumnTypeQuery(tableName: String, columnName: String, newDataType: String): String = {
    s"ALTER TABLE $tableName MODIFY COLUMN ${quoteIdentifier(columnName)} $newDataType"
  }

  // See https://mariadb.com/kb/en/alter-table/
  override def getRenameColumnQuery(tableName: String, columnName: String, newName: String, dbMajorVersion: Int): String = {
    s"ALTER TABLE $tableName RENAME COLUMN ${quoteIdentifier(columnName)} TO ${quoteIdentifier(newName)}"
  }

  // It is required to have column data type to change the column nullability, see https://mariadb.com/kb/en/alter-table/:
  // ALTER TABLE tbl_name MODIFY [COLUMN] col_name column_definition
  // column_definition:
  //    data_type [NOT NULL | NULL]
  // e.g. ALTER TABLE t1 MODIFY b INT NOT NULL;
  // We don't have column data type here, so throw an exception for now.
  override def getUpdateColumnNullabilityQuery(tableName: String, columnName: String, isNullable: Boolean): String = {
    throw new SQLFeatureNotSupportedException("UpdateColumnNullability is not supported without knowledge of the data type.")
  }

  // See https://mariadb.com/kb/en/alter-table/
  override def getTableCommentQuery(table: String, comment: String): String = {
    s"ALTER TABLE $table COMMENT = '$comment'"
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    // See SPARK-35446: MySQL treats REAL as a synonym to DOUBLE by default.
    // The same holds for MariaDB, see https://mariadb.com/kb/en/double-precision/.
    // We override getJDBCType so that FloatType is mapped to FLOAT instead.
    case FloatType => Option(JdbcType("FLOAT", java.sql.Types.FLOAT))
    case _ => JdbcUtils.getCommonJDBCType(dt)
  }

  override def getSchemaCommentQuery(schema: String, comment: String): String = {
    throw new SQLFeatureNotSupportedException("Schema comments are not supported in MariaDB.")
  }

  override def removeSchemaCommentQuery(schema: String): String = {
    throw new SQLFeatureNotSupportedException("Schema comments are not supported in MariaDB.")
  }

  // See c
  override def createIndex(indexName: String, tableIdent: Identifier, columns: Array[NamedReference],
                           columnsProperties: java.util.Map[NamedReference, java.util.Map[String, String]],
                           properties: java.util.Map[String, String]): String = {
    val columnList = columns.map(col => quoteIdentifier(col.fieldNames.head))
    val (indexType, indexPropertyList) = JdbcUtils.processIndexProperties(properties, "mysql")

    s"CREATE INDEX ${quoteIdentifier(indexName)} $indexType ON" +
      s" ${quoteIdentifier(tableIdent.name())} (${columnList.mkString(", ")})" +
      s" ${indexPropertyList.mkString(" ")}"
  }

  // See https://mariadb.com/kb/en/show-index/
  override def indexExists(conn: java.sql.Connection, indexName: String, tableIdent: Identifier, options: JDBCOptions): Boolean = {
    val sql = s"SHOW INDEXES FROM ${quoteIdentifier(tableIdent.name())} WHERE key_name = '$indexName'"
    JdbcUtils.checkIfIndexExists(conn, sql, options)
  }

  // See https://mariadb.com/kb/en/drop-index/
  override def dropIndex(indexName: String, tableIdent: Identifier): String = {
    s"DROP INDEX ${quoteIdentifier(indexName)} ON ${tableIdent.name()}"
  }

  override def dropSchema(schema: String, cascade: Boolean): String = {
    if (cascade) {
      s"DROP SCHEMA ${quoteIdentifier(schema)}"
    } else {
      throw new SQLFeatureNotSupportedException("DROP SCHEMA is always cascading in MariaDB.")
    }
  }
}
