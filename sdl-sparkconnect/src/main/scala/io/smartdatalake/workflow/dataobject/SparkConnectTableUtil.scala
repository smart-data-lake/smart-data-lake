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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.{ProductUtil, SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.GenericColumn
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectColumn, SparkConnectDataFrame, SparkConnectSubFeed}
import io.smartdatalake.workflow.dataobject.generic.Table
import org.apache.spark.sql.functions.{col, expr, lit}
import org.apache.spark.sql.{Column, DataFrame, DataFrameWriterV2, Row, SaveMode, SparkSession}

/**
 * Shared logic for table operations through a Spark Connect session,
 * used by [[SparkConnectTableDataObject]] and [[DeltaLakeTableSparkConnectEngine]].
 */
private[smartdatalake] object SparkConnectTableUtil extends SmartDataLakeLogger {

  /**
   * Merges DataFrame with existing table data by using the Spark native merge API.
   *
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   *
   * This all is done in one transaction.
   * Note that the table format needs to support row-level operations on the server side, e.g. delta or iceberg.
   */
  def mergeDataFrameByPrimaryKey(session: SparkSession, df: DataFrame, table: Table, saveModeOptions: SaveModeMergeOptions, allowSchemaEvolution: Boolean, id: DataObjectId): MetricsMap = {
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")
    val tableName = table.name
    val saveModeExpr = saveModeOptions.getExpressions(SparkConnectSubFeed.subFeedType, existingAliasReplacement = Some(tableName))
    def toSpark(expr: GenericColumn): Column = expr.asInstanceOf[SparkConnectColumn].inner
    val insertCols = df.columns.diff(saveModeOptions.insertColumnsToIgnore)
    val existingCols = session.table(table.fullName).columns
    val additionalCols = insertCols.diff(existingCols)

    // prepare join condition
    val joinCondition = table.primaryKey.get.map(colName => col(s"new.$colName") === col(s"$tableName.$colName")).reduce(_ and _)
    var mergeStmt = df.as("new")
      .mergeInto(table.fullName, joinCondition and saveModeExpr.additionalMergePredicateExpr.map(toSpark).getOrElse(lit(true)))

    // enable schema evolution
    if (allowSchemaEvolution) {
      mergeStmt = mergeStmt.withSchemaEvolution() // does not work in Spark 4.1
      // workaround for delta: set this globally
      session.conf.set(key = "spark.databricks.delta.schema.autoMerge.enabled", value = true)
    }

    // delete clause if configured
    saveModeExpr.deleteConditionExpr.map(toSpark).foreach(c => mergeStmt = mergeStmt.whenMatched(c).delete())

    // update clause
    if (saveModeOptions.updateColumnsOpt.isDefined) {
      val updateCols = saveModeOptions.updateColumnsOpt.getOrElse(df.columns.toSeq.diff(table.primaryKey.get))
      mergeStmt = mergeStmt.whenMatched(saveModeExpr.updateConditionExpr.map(toSpark).getOrElse(lit(true))).update(updateCols.map(c => c -> col(s"new.$c")).toMap)
    } else {
      mergeStmt = mergeStmt.whenMatched(saveModeExpr.updateConditionExpr.map(toSpark).getOrElse(lit(true))).updateAll()
    }

    // update existing clause if configured
    if (saveModeOptions.updateExistingCondition.isDefined) {
      val updateCols = df.columns.toSeq.diff(Seq(Historization.historizeOperationColName)).diff(additionalCols)
      mergeStmt = mergeStmt.whenMatched(saveModeExpr.updateExistingConditionExpr.map(toSpark).getOrElse(lit(true))).update(updateCols.map(c => c -> col(s"new.$c")).toMap)
    }

    // insert clause
    if (saveModeOptions.insertColumnsToIgnore.nonEmpty || saveModeOptions.insertValuesOverride.nonEmpty) {
      mergeStmt = mergeStmt.whenNotMatched(saveModeExpr.insertConditionExpr.map(toSpark).getOrElse(lit(true)))
        .insert(insertCols.map(c => c -> saveModeOptions.insertValuesOverride.get(c).map(lit).getOrElse(col(s"new.$c"))).toMap)
    } else {
      mergeStmt = mergeStmt.whenNotMatched(saveModeExpr.insertConditionExpr.map(toSpark).getOrElse(lit(true))).insertAll()
    }

    // execute merge statement
    logger.info(s"($id) executing merge statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1 + "=" + e._2).mkString(" ")}")
    mergeStmt.merge()
    // Note: there is no QueryExecutionListener to collect metrics on the Spark Connect client side.
    Map()
  }

  /**
   * Listing partitions by a "select distinct partition-columns" query
   */
  def listPartitions(session: SparkSession, table: Table, partitions: Seq[String]): Seq[PartitionValues] = {
    PartitionValues.fromDataFrame(SparkConnectDataFrame(session.table(table.fullName).select(partitions.map(col): _*).distinct()))
  }

  /**
   * Delete partition data with a SQL delete statement.
   * Note that this needs a table format supporting row-level operations on the server side, e.g. delta or iceberg.
   */
  def deletePartitions(session: SparkSession, table: Table, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    if (partitionValues.nonEmpty) {
      session.sql(SQLUtil.createDeletePartitionStatement(table.fullName, partitionValues, SQLUtil.sparkQuoteCaseSensitiveColumn(_))).collect()
    }
  }

  /**
   * Move partition data with a SQL update statement.
   * Note that this needs a table format supporting row-level operations on the server side, e.g. delta or iceberg.
   */
  def movePartitions(session: SparkSession, table: Table, partitionValues: Seq[(PartitionValues, PartitionValues)], id: DataObjectId)(implicit context: ActionPipelineContext): Unit = {
    partitionValues.foreach {
      case (pvExisting, pvNew) =>
        val updateSpec = pvNew.elements.map { case (k, v) => s"${SQLUtil.sparkQuoteCaseSensitiveColumn(k)} = '${v.toString.replace("'", "''")}'" }.mkString(", ")
        val filter = pvExisting.elements.map { case (k, v) => s"${SQLUtil.sparkQuoteCaseSensitiveColumn(k)} = '${v.toString.replace("'", "''")}'" }.mkString(" AND ")
        session.sql(s"UPDATE ${table.fullName} SET $updateSpec WHERE $filter").collect()
        logger.info(s"($id) Partition $pvExisting moved to $pvNew")
    }
  }

  /**
   * Execute a write with the DataFrameWriterV2 API according to the given [[SDLSaveMode]].
   * This is the Spark Connect twin of [[io.smartdatalake.definitions.SparkSaveModeUtil.execV2]],
   * which is typed on the classic DataFrameWriterV2.
   * Note that this needs a table format supporting the DSv2 write API on the server side, e.g. iceberg.
   */
  def execV2(saveMode: SDLSaveMode, writer: DataFrameWriterV2[Row], partitionValues: Seq[PartitionValues], partitionOverwriteModeDynamic: Boolean = false): Unit = {
    saveMode match {
      case SDLSaveMode.Append => writer.append()
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.nonEmpty =>
        val filterSql = partitionValues.map(_.getFilterExprSql).mkString("(", ") OR (", ")")
        writer.overwrite(expr(filterSql))
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.isEmpty && partitionOverwriteModeDynamic => writer.overwritePartitions()
      case SDLSaveMode.Overwrite | SDLSaveMode.OverwriteOptimized if partitionValues.isEmpty => writer.replace()
    }
  }

  /**
   * Dynamic partition overwrite: overwrite the partitions contained in the DataFrame using insertInto.
   * Throws ProcessingLogicException if partitionOverwriteMode=dynamic is not configured, as a protection from unintentionally deleting all partition data.
   */
  def insertIntoDynamicPartitionOverwrite(session: SparkSession, targetDf: DataFrame, table: Table, options: Map[String, String], id: DataObjectId, typeName: String): Unit = {
    val overwriteModeIsDynamic = options.get("partitionOverwriteMode").orElse(session.conf.getOption("spark.sql.sources.partitionOverwriteMode")).contains("dynamic")
    if (!overwriteModeIsDynamic) throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data. Set option.partitionOverwriteMode=dynamic on this $typeName to enable dynamic partition overwrite and get around this exception.")
    // insertInto is position-based, reorder DataFrame columns to the columns of the existing table
    val tableCols = session.table(table.fullName).columns.toSeq
    // the partitionOverwriteMode needs to be set as session conf, as writer options are not honored by insertInto for all table formats
    val previousOverwriteMode = session.conf.getOption("spark.sql.sources.partitionOverwriteMode")
    session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    try targetDf.select(tableCols.map(col): _*).write.options(options)
      .mode(SaveMode.Overwrite).insertInto(table.fullName)
    finally previousOverwriteMode.foreach(session.conf.set("spark.sql.sources.partitionOverwriteMode", _))
  }
}
