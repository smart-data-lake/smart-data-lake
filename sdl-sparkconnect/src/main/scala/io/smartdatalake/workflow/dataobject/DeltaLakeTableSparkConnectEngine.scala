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

import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions, SaveModeOptions, TableStatsType}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{PerformanceUtils, SmartDataLakeLogger}
import io.smartdatalake.utils.sparkconnect.ReadWrite
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectDataFrame, SparkConnectSchema, SparkConnectSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, max, rank}
import org.apache.spark.sql._

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}
import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Spark Connect engine implementation of [[DeltaLakeTableDataObject]], working against a remote Spark Connect server with delta support.
 *
 * Limitations compared to the classic Spark engine:
 * - no column statistics (needs DeltaLog internals)
 * - no Spark stage metrics (no QueryExecutionListener on the Spark Connect client side), delta operation metrics only
 * - schemaMin metadata (column comments) are not merged into the written schema
 */
class DeltaLakeTableSparkConnectEngine(dataObject: DeltaLakeTableDataObject) extends DeltaLakeTableEngine with ReadWrite with SmartDataLakeLogger {

  override val subFeedType: Type = typeOf[SparkConnectSubFeed]

  private def id = dataObject.id
  private def table = dataObject.table

  // remember the session for methods called without context, e.g. getState
  @transient private var _session: Option[SparkSession] = None
  private def session(implicit context: ActionPipelineContext): SparkSession = {
    val resolvedSession = SparkConnectSubFeed.getSparkSession
    _session = Some(resolvedSession)
    resolvedSession
  }

  override def prepare()(implicit context: ActionPipelineContext): Unit = {
    // no spark.sql.extensions check - delta support is a server-side concern with Spark Connect
    if (dataObject.path.isDefined) logger.warn(s"($id) path is handled server-side with the Spark Connect engine; filesystem checks, path repair and convertToDelta are skipped")
  }

  private def propertyExists(name: String)(implicit context: ActionPipelineContext): Boolean = {
    val properties = session.sql(s"DESCRIBE DETAIL ${table.fullName}").select("properties").head().getMap[String, String](0)
    properties.contains(name)
  }

  private def propertyExistsWithValue(name: String, value: String)(implicit context: ActionPipelineContext): Boolean = {
    val properties = session.sql(s"DESCRIBE DETAIL ${table.fullName}").select("properties").head().getMap[String, String](0)
    properties.exists(_ == name -> value)
  }

  private def activateCdc()(implicit context: ActionPipelineContext): Unit = {
    if (!propertyExists(enableCdcFeedProperty) && isTableExisting)
      session.sql(s"ALTER TABLE ${table.fullName} SET TBLPROPERTIES ('$enableCdcFeedProperty' = 'true')").collect()
  }

  @transient private val enableCdcFeedProperty = "delta.enableChangeDataFeed"

  override def getDataFrame(partitionValues: Seq[PartitionValues], incrementalOutputExpr: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame = {

    val cdcActivated = incrementalOutputExpr.isDefined && propertyExistsWithValue(enableCdcFeedProperty, "true")

    val df = if (incrementalOutputExpr.isDefined) {

      require(table.primaryKey.isDefined, s"($id) PrimaryKey for table [${table.fullName}] needs to be defined when using DataObjectStateIncrementalMode")

      val df = if (cdcActivated && !incrementalOutputExpr.contains("0") ) {

        val windowSpec = Window.partitionBy(table.primaryKey.get.map(col).toIndexedSeq: _*).orderBy(col("_commit_timestamp").desc)

        session.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", incrementalOutputExpr.get)
          .table(table.fullName)
          .where(col("_change_type").isin("insert", "update_postimage"))
          .withColumn("_rank", rank().over(windowSpec))
          .where(col("_rank") === 1)
          .drop("_rank", "_change_type", "_commit_version", "_commit_timestamp")

      } else {
        if (!cdcActivated) activateCdc()
        session.read.options(dataObject.options).table(table.fullName)
      }

      dataObject.incrementalOutputExpr = getLatestVersion.map(v => (v + 1).toString) // version to read from next time
      logger.info(s"($id) incrementalOutputExpr=" + dataObject.incrementalOutputExpr)

      df
    } else {
      session.read.options(dataObject.options).table(table.fullName)
    }

    SparkConnectDataFrame(df)
  }

  override def writeDataFrame(genericDf: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val helper: SparkConnectSubFeed.type = SparkConnectSubFeed

    val sparkDf = genericDf match {
      case d: SparkConnectDataFrame => d
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(genericDf)
    }
    val df = sparkDf.inner
    // remove columns from DataFrame which are only needed for merge operation, e.g. columns listed in insertColumnsToIgnore
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(sparkDf)).getOrElse(sparkDf).inner
    if (dataObject.schemaMin.isDefined) {
      dataObject.validateSchemaMin(SparkConnectSchema(targetDf.schema), "write")
      logger.debug(s"($id) schemaMin metadata (column comments) are not merged into the written schema with the Spark Connect engine")
    }
    dataObject.validateSchemaHasPartitionCols(targetDf.columns.toIndexedSeq, "write")
    dataObject.validateSchemaHasPrimaryKeyCols(targetDf.columns.toIndexedSeq, "write")

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(dataObject.saveMode)

    val userMetadata = s"${context.application} runId=${context.executionId.runId} attemptId=${context.executionId.attemptId}"
    session.conf.set("spark.databricks.delta.commitInfo.userMetadata", userMetadata)

    def newDfWriter(d: DataFrame): DataFrameWriter[Row] = {
      d.write
        .format("delta")
        .options(dataObject.options)
        .option("userMetadata", userMetadata)
        .option("mergeSchema", dataObject.allowSchemaEvolution) // allow schema evolution for SaveMode.Append
    }

    if (isTableExisting) {
      if (!dataObject.allowSchemaEvolution) dataObject.validateSchema(SparkConnectSchema(targetDf.schema), SparkConnectSchema(session.table(table.fullName).schema), "write")
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions.
        // Therefore, df instead of targetDf is passed on.
        SparkConnectTableUtil.mergeDataFrameByPrimaryKey(session, df, table, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()), dataObject.allowSchemaEvolution, id)
      } else if (dataObject.partitions.isEmpty) {
        // overwrite all
        newDfWriter(targetDf)
          .option("overwriteSchema", dataObject.allowSchemaEvolution) // allow overwriting schema when overwriting whole table
          .mode(SparkConnectTableDataObject.sparkSaveMode(finalSaveMode))
          .saveAsTable(table.fullName)
      } else if (finalSaveMode == SDLSaveMode.Overwrite) {
        if (partitionValues.nonEmpty) {
          newDfWriter(targetDf)
            .conditionalOption("replaceWhere", partitionValues.nonEmpty, () => partitionValues.map(pv => s"(${pv.getFilterExprSql})").mkString(" OR "))
            .conditionalOption("partitionOverwriteMode", partitionValues.nonEmpty, () => "static") // reset partitionOverwriteMode=dynamic when using replaceWhere
            .mode(SaveMode.Overwrite).saveAsTable(table.fullName)
        } else {
          // dynamic partition overwrite: overwrite the partitions contained in the DataFrame
          SparkConnectTableUtil.insertIntoDynamicPartitionOverwrite(session, targetDf, table, dataObject.options, id, dataObject.getClass.getSimpleName)
        }
      } else {
        // insert append
        newDfWriter(targetDf)
          .mode(SparkConnectTableDataObject.sparkSaveMode(finalSaveMode))
          .saveAsTable(table.fullName)
      }
    } else {
      // create new table
      var dfWriter = newDfWriter(targetDf)
      if (dataObject.partitions.nonEmpty) dfWriter = dfWriter.partitionBy(dataObject.partitions: _*)
      // Note: for external tables the path is resolved client-side; configure an absolute path incl. scheme to make sure it is valid on the server.
      if (dataObject.path.isDefined) dfWriter = dfWriter.option("path", dataObject.hadoopPath.toString)
      dfWriter.saveAsTable(table.fullName)
    }

    if (dataObject.updateColumnComments) logger.warn(s"($id) updateColumnComments is not supported with the Spark Connect engine")

    // get delta table operational metrics
    // Note: there is no QueryExecutionListener to collect Spark stage metrics on the Spark Connect client side, delta operation metrics only.
    val latestHistoryEntry = session.sql(s"DESCRIBE HISTORY ${table.fullName} LIMIT 1").select("operationMetrics", "userMetadata").head()
    if (latestHistoryEntry.getString(1) != userMetadata) {
      logger.info(s"($id) No new version was written. No data was written to this DeltaLake table.")
      throw NoDataToProcessWarning(id.id, s"($id) No data was written to DeltaLake table by Spark.")
    }
    DeltaLakeTableDataObject.normalizeDeltaMetrics(latestHistoryEntry.getMap[String, String](0))
  }

  override def vacuum()(implicit context: ActionPipelineContext): Unit = {

    def intervalHasPassed(lastExecution: Timestamp): Boolean = {
      val timePassed = Duration.between(lastExecution.toLocalDateTime, LocalDateTime.now)
      timePassed.compareTo(Duration.parse(dataObject.minVacuumInterval.get)) > 0 //the time passed is greater than the set minInterval
    }

    lazy val lastVacuum = session.sql(s"DESCRIBE HISTORY ${table.fullName}")
      .filter(col("operation").contains("VACUUM END")).select(max(col("timestamp"))).collect()

    //execute vacuum if either no interval is set, there has never been a vacuum operation, or the set interval has passed
    if (dataObject.minVacuumInterval.isEmpty || lastVacuum.isEmpty || lastVacuum(0).isNullAt(0) || intervalHasPassed(lastVacuum(0).getTimestamp(0))) {
      dataObject.retentionPeriod.foreach { period =>
        val (_, d) = PerformanceUtils.measureDuration {
          session.sql(s"VACUUM ${table.fullName} RETAIN $period HOURS").collect()
        }
        logger.info(s"($id) vacuum took $d")
      }
    }
  }

  // cache response to avoid remote catalog query.
  @transient private var cachedIsDbExisting: Option[Boolean] = None
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsDbExisting.getOrElse {
      cachedIsDbExisting = Option(table.db.forall(session.catalog.databaseExists))
      cachedIsDbExisting.get
    }
  }

  // cache if table is existing to avoid remote catalog query.
  @transient private var cachedIsTableExisting: Option[Boolean] = None
  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsTableExisting.getOrElse {
      val existing = session.catalog.tableExists(table.fullName)
      if (existing) cachedIsTableExisting = Some(existing) // only cache if existing, otherwise query again later
      existing
    }
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    session.sql(s"DROP TABLE IF EXISTS ${table.fullName}").collect()
    if (dataObject.path.isDefined) logger.warn(s"($id) the path of the external table is not deleted with the Spark Connect engine")
    cachedIsTableExisting = None
  }

  override def getTableLocation(implicit context: ActionPipelineContext): String = {
    session.sql(s"DESCRIBE DETAIL ${table.fullName}").select("location").head().getString(0)
  }

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    val (pvs, d) = PerformanceUtils.measureDuration(
      if (dataObject.partitions.nonEmpty && isTableExisting) SparkConnectTableUtil.listPartitions(session, table, dataObject.partitions)
      else Seq()
    )
    logger.debug(s"($id) listPartitions took $d")
    pvs
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    SparkConnectTableUtil.deletePartitions(session, table, partitionValues)
  }

  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    SparkConnectTableUtil.movePartitions(session, table, partitionValues, id)
  }

  override def getStats(update: Boolean)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      val historyRow = session.sql(s"DESCRIBE HISTORY ${table.fullName} LIMIT 1")
        .selectExpr("unix_millis(timestamp) as ts", "userMetadata").head()
      val lastCommitMsg = historyRow.getString(1)
      val oldestSnapshot = historyRow.getLong(0)
      val detailRow = session.sql(s"DESCRIBE DETAIL ${table.fullName}")
        .selectExpr("unix_millis(createdAt) as createdAt", "unix_millis(lastModified) as lastModified", "numFiles", "sizeInBytes").head()
      val (createdAt, lastModifiedAt, numDataFilesCurrent, sizeInBytesCurrent) = (detailRow.getLong(0), detailRow.getLong(1), detailRow.getLong(2), detailRow.getLong(3))
      val numRows = session.table(table.fullName).count()
      val deltaStats = Map(TableStatsType.CreatedAt.toString -> createdAt, TableStatsType.LastModifiedAt.toString -> lastModifiedAt, TableStatsType.LastCommitMsg.toString -> lastCommitMsg, TableStatsType.NumDataFilesCurrent.toString -> numDataFilesCurrent, TableStatsType.SizeInBytesCurrent.toString -> sizeInBytesCurrent, TableStatsType.OldestSnapshotTs.toString -> oldestSnapshot, TableStatsType.NumRows.toString -> numRows)
      // no Hadoop path stats with the Spark Connect engine (no client filesystem access to the table location)
      val columnStats = getColumnStats(update, Some(lastModifiedAt))
      deltaStats ++ dataObject.getPartitionStats + (TableStatsType.Columns.toString -> columnStats)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get table stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map("info" -> e.getMessage)
    }
  }

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])(implicit context: ActionPipelineContext): Map[String, Map[String, Any]] = {
    logger.info(s"($id) getColumnStats is not supported with the Spark Connect engine, returning empty stats")
    Map()
  }

  /**
   * Return the last table version
   */
  def getLatestVersion: Option[Long] = {
    // no context available here - use the session remembered from previous calls (reading/writing the table always happens before getState)
    val stateSession = _session
      .getOrElse(throw new IllegalStateException(s"($id) No Spark Connect session available to get state."))
    val dfHistory = stateSession.sql(s"DESCRIBE HISTORY ${table.fullName} LIMIT 1")
    val latestVersion = dfHistory.select("version").head().getAs[Long](0)
    Option(latestVersion)
  }

  override def sql(stmt: String)(implicit context: ActionPipelineContext): GenericDataFrame = {
    SparkConnectDataFrame(session.sql(stmt))
  }
}
