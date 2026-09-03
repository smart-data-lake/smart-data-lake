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
package io.smartdatalake.util.misc

import io.smartdatalake.definitions.{Environment, SaveModeMergeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.generic.{DataObjectEngine, ForeignKeyDefinition, Table}
import org.slf4j.Logger

object SQLUtil extends SmartDataLakeLogger {

  /**
   * Create a SQL delete statement for given partition values.
   */
  def createDeletePartitionStatement(tableName: String, partitionValues: Seq[PartitionValues], quoteCaseSensitiveColumn: String => String): String = {
    assert(partitionValues.nonEmpty)
    val partitionsColss = partitionValues.map(_.keys).distinct
    assert(partitionsColss.size == 1, "All partition values must have the same set of partition columns defined!")
    val partitionCols = partitionsColss.head
    if (partitionCols.size == 1) {
      s"delete from $tableName where ${quoteCaseSensitiveColumn(partitionCols.head)} in ('${partitionValues.map(pv => pv(partitionCols.head)).mkString("','")}')"
    } else {
      val partitionValuesStr = partitionValues.map(pv => s"(${partitionCols.map(c => s"'${pv(c).toString}'").mkString(",")})")
      s"delete from $tableName where (${partitionCols.map(quoteCaseSensitiveColumn).mkString(",")}) in (${partitionValuesStr.mkString(",")})"
    }
  }

  /**
   * Create a SQL update statement to move all records of one partition into another partition, by updating
   * the value of the partition columns. This is used to archive partitions by housekeeping, see [[io.smartdatalake.workflow.dataobject.generic.PartitionArchiveMode]].
   *
   * Note that for table based DataObjects no data is moved physically, only the value of the partition columns changes.
   * If the target partition exists already, the records are merged into it.
   */
  def createMovePartitionStatement(tableName: String, partitionValuesFrom: PartitionValues, partitionValuesTo: PartitionValues, quoteCaseSensitiveColumn: String => String): String = {
    assert(partitionValuesFrom.nonEmpty, "Partition values to move must not be empty!")
    assert(partitionValuesFrom.keys == partitionValuesTo.keys,
      s"Source and target partition values must have the same set of partition columns defined ($partitionValuesFrom -> $partitionValuesTo)!")
    val partitionCols = partitionValuesFrom.keys.toSeq
    def condition(pv: PartitionValues, col: String) = s"${quoteCaseSensitiveColumn(col)} = '${escapeSqlStringLiteral(pv(col).toString)}'"
    val setStr = partitionCols.map(c => condition(partitionValuesTo, c)).mkString(", ")
    val whereStr = partitionCols.map(c => condition(partitionValuesFrom, c)).mkString(" and ")
    s"update $tableName set $setStr where $whereStr"
  }

  /**
   * Create a SQL merge statement for given saveModeOptions
   */
  def createMergeStatement(targetTable: Table, columns: Seq[String], tmpTableName: String, saveModeOptions: SaveModeMergeOptions, quoteCaseSensitiveColumn: String => String): String = {
    val additionalMergePredicateStr = saveModeOptions.additionalMergePredicate.map(p => s" AND $p").getOrElse("")
    val joinConditionStr = targetTable.primaryKey.get.map(quoteCaseSensitiveColumn).map(colName => s"new.$colName = existing.$colName").reduce(_+" AND "+_)
    val deleteClauseStr = saveModeOptions.deleteCondition.map(c => s"\nWHEN MATCHED AND $c THEN DELETE").getOrElse("")
    val updateConditionStr = saveModeOptions.updateCondition.map(c => s" AND $c").getOrElse("")
    val updateSpecStr = saveModeOptions.updateColumnsOpt.getOrElse(columns).diff(targetTable.primaryKey.get).map(quoteCaseSensitiveColumn).map(colName => s"existing.$colName = new.$colName").reduce(_+", "+_)
    val insertConditionStr = saveModeOptions.insertCondition.map(c => s" AND $c").getOrElse("")
    val insertCols = columns.diff(saveModeOptions.insertColumnsToIgnore)
    val insertSpecStr = insertCols.map(quoteCaseSensitiveColumn).reduce(_+", "+_)
    val insertValueSpecStr = insertCols.map(colName => saveModeOptions.insertValuesOverride.getOrElse(colName, s"new.${quoteCaseSensitiveColumn(colName)}")).reduce(_+", "+_)
    s"""
    | MERGE INTO ${targetTable.fullName} as existing
    | USING (SELECT * from $tmpTableName) as new
    | ON $joinConditionStr $additionalMergePredicateStr $deleteClauseStr
    | WHEN MATCHED $updateConditionStr THEN UPDATE SET $updateSpecStr
    | WHEN NOT MATCHED $insertConditionStr THEN INSERT ($insertSpecStr) VALUES ($insertValueSpecStr)
    """.stripMargin
  }

  def createUpdateExistingStatement(targetTable: Table, columns: Seq[String], tmpTableName: String, saveModeOptions: SaveModeMergeOptions, quoteCaseSensitiveColumn: String => String): Option[String] = {

    if (saveModeOptions.updateExistingCondition.isDefined) {
      val additionalMergePredicateStr = saveModeOptions.additionalMergePredicate.map(p => s" AND $p").getOrElse("")
      val joinConditionStr = targetTable.primaryKey.get.map(quoteCaseSensitiveColumn).map(colName => s"new.$colName = existing.$colName").reduce(_ + " AND " + _)
      val updateExistingConditionStr = saveModeOptions.updateExistingCondition.map(c => s" AND $c").getOrElse("")
      // columns which are not inserted into the target table do not exist there and can not be updated either
      val updateExistingCols = columns.diff(Seq(Historization.historizeOperationColName)).diff(saveModeOptions.insertColumnsToIgnore)
      val updateExistingSpecStr = updateExistingCols.map(colName => s"existing.$colName = new.$colName").reduce(_ + ", " + _)

      Some(s"""
         | MERGE INTO ${targetTable.fullName} as existing
         | USING (SELECT * from $tmpTableName) as new
         | ON $joinConditionStr $additionalMergePredicateStr
         | WHEN MATCHED $updateExistingConditionStr THEN UPDATE SET $updateExistingSpecStr

    """.stripMargin)
    } else None
  }

  /**
   * Quote column name if spark is in case sensitive mode, or the name includes special characters.
   */
  def sparkQuoteCaseSensitiveColumn(column: String)(implicit context: ActionPipelineContext): String = {
    if (Environment.caseSensitive) sparkQuoteSQLIdentifier(column)
    else {
      // quote identifier if it contains special characters
      if (hasIdentifierSpecialChars(column)) sparkQuoteSQLIdentifier(column)
      else column
    }
  }

  /**
   * Check if column name includes non SQL standard characters.
   */
  def hasIdentifierSpecialChars(colName: String): Boolean = {
    !colName.matches("[a-zA-Z][a-zA-Z0-9_]*")
  }

  /**
   * Quote column name for spark.
   */
  def sparkQuoteSQLIdentifier(column: String): String = {
    s"`$column`"
  }

  /**
   * Create a SQL statement to add a foreign key constraint.
   * The columns of the foreign key and the referenced columns are listed in the same order,
   * see [[ForeignKeyDefinition.columns]].
   */
  def createForeignKeyStatement(tableName: String, foreignKey: ForeignKeyDefinition, quoteCaseSensitiveColumn: String => String = identity): String = {
    val constraintName = foreignKey.name.getOrElse(throw new IllegalArgumentException(s"Constraint name missing for ${foreignKey.describe}"))
    val columns = foreignKey.columns.toSeq
    s"ALTER TABLE $tableName ADD CONSTRAINT $constraintName" +
      s" FOREIGN KEY (${columns.map(c => quoteCaseSensitiveColumn(c._1)).mkString(", ")})" +
      s" REFERENCES ${foreignKey.referencedTable} (${columns.map(c => quoteCaseSensitiveColumn(c._2)).mkString(", ")})"
  }

  /**
   * Escape a string to be used as SQL string literal, e.g. a table or column comment.
   */
  def escapeSqlStringLiteral(str: String): String = str.replace("'", "''")

  /**
   * Execute a SQL statement as is.
   * Use this for SDLB generated statements, which address the table by its fully qualified name, see [[Table.fullName]].
   */
  def execSql(stmt: String, execFun: String => Unit, loggerContext: String): Unit = {
    try {
      logger.info(s"${loggerContext}Executing SQL statement: $stmt")
      execFun(stmt)
    } catch {
      case e: Exception =>
        logger.warn(s"${loggerContext}Error in SQL statement '$stmt':\n${e.getMessage}")
        throw e
    }
  }

  /**
   * Execute a user defined SQL statement, prefixed by a USE statement based on the configured table.
   * This allows the user to use unqualified table names in the statement, which are then resolved
   * against the catalog and schema of the configured table.
   *
   * Note that the USE statement changes the current catalog and schema of the shared session for all
   * statements following in that session. For SDLB generated statements use [[execSql]] with a fully
   * qualified table name instead.
   */
  def execSqlBasedOnTable(stmt: String, table: Table, execSql: String => Unit, loggerContext: String)(implicit context: ActionPipelineContext): Unit = {
    try {
      // Note that "USE <catalog>.<schema>" is supported by open source Spark and Databricks,
      // in contrast to "USE CATALOG <catalog>" which is Databricks/Unity Catalog syntax only.
      val newStmt = Seq(
        Some(table.getDbName).filter(_.nonEmpty).map(namespace => s"USE $namespace"),
        Some(stmt)
      ).flatten
      logger.info(s"${loggerContext}Executing SQL statements: ${newStmt.mkString("; ")}")
      newStmt.foreach(execSql(_))
    } catch {
      case e: Exception =>
        logger.warn(s"${loggerContext}Error in SQL statement '$stmt':\n${e.getMessage}")
        throw e
    }
  }
}
