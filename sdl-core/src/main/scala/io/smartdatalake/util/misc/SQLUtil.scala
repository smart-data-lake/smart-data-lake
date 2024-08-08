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

package io.smartdatalake.util.misc

import io.smartdatalake.definitions.{Environment, SaveModeMergeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataobject.Table

object SQLUtil {

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
      val updateExistingSpecStr = columns.diff(Seq(Historization.historizeOperationColName)).map(colName => s"existing.$colName = new.$colName").reduce(_ + ", " + _)

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
}
