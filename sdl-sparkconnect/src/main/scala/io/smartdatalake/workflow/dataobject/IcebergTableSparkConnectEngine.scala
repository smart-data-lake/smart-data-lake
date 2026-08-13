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

import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeMergeOptions, SaveModeOptions, ColumnStatsType, TableStatsType}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{PerformanceUtils, SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.utils.sparkconnect.ReadWrite
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.sparkconnect.{SparkConnectDataFrame, SparkConnectSchema, SparkConnectSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, max, min, rank, sum}
import org.apache.spark.sql.{Row, SparkSession}

import java.sql.Timestamp
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.Try

/**
 * Spark Connect engine implementation of [[IcebergTableDataObject]], working against a remote Spark Connect server
 * with Iceberg support. Everything is done through SQL, Iceberg stored procedures and Iceberg metadata tables,
 * there is no dependency on the Iceberg Java API (which needs driver-side catalog access).
 *
 * Limitations compared to the classic Spark engine:
 * - no Spark stage metrics (no QueryExecutionListener on the Spark Connect client side), Iceberg snapshot metrics only
 * - no filesystem based table maintenance in prepare: an existing path is neither registered nor converted to Iceberg
 *   format, and a table with a missing path is not dropped (there is no client-side filesystem access to the table location)
 * - no Hadoop path statistics in getStats
 * - schema evolution on merge is handled by the servers merge implementation instead of the Iceberg updateSchema API
 */
class IcebergTableSparkConnectEngine(dataObject: IcebergTableDataObject) extends IcebergTableEngine
  with ReadWrite with SmartDataLakeLogger {

  override val subFeedType: Type = typeOf[SparkConnectSubFeed]

  private def id = dataObject.id
  private def table = dataObject.table

  private def session(implicit context: ActionPipelineContext): SparkSession = SparkConnectSubFeed.getSparkSession

  /** catalog to be used for Iceberg stored procedure calls, e.g. CALL <catalog>.system.expire_snapshots */
  private def catalogName: String = table.catalog.getOrElse("spark_catalog")

  /** table identifier without catalog, as expected by the Iceberg stored procedures */
  private def identifierName: String = Seq(table.db, Some(table.name)).flatten.mkString(".")

  /** Iceberg table property to allow writing a DataFrame with a different schema, see org.apache.iceberg.TableProperties */
  private val acceptAnySchemaProperty = "write.spark.accept-any-schema"

  /** Iceberg write option to merge the schema of the written DataFrame into the table, see org.apache.iceberg.spark.SparkWriteOptions */
  private val mergeSchemaOption = "merge-schema"

  private def updateTableProperty(name: String, value: String)(implicit context: ActionPipelineContext): Unit = {
    session.sql(s"ALTER TABLE ${table.fullName} SET TBLPROPERTIES ('$name' = '$value')").collect()
    logger.info(s"($id) updated Iceberg table property $name to $value")
  }

  override def prepare()(implicit context: ActionPipelineContext): Unit = {
    // no spark.sql.extensions check - Iceberg support is a server-side concern with Spark Connect
    if (dataObject.path.isDefined) logger.warn(s"($id) path is handled server-side with the Spark Connect engine; filesystem checks, registering and converting an existing path to Iceberg format are skipped")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues], incrementalOutputExpr: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    val df = incrementalOutputExpr match {
      case Some(snapshotId) =>
        require(table.primaryKey.isDefined, s"($id) PrimaryKey for table [${table.fullName}] needs to be defined when using DataObjectStateIncrementalMode")
        val icebergTable = if (snapshotId == "0") session.read.options(dataObject.options).table(table.fullName)
        else {

          // activate temporary cdc view
          session.sql(
            s"""CALL $catalogName.system.create_changelog_view(table => '$identifierName'
               |, options => map('start-snapshot-id', '$snapshotId')
               |, compute_updates => true
               |, identifier_columns => array('${table.primaryKey.get.mkString("','")}')
               |)""".stripMargin).collect()

          // read cdc events
          val temporaryViewName = table.name + "_changes"

          val windowSpec = Window.partitionBy(table.primaryKey.get.map(col).toIndexedSeq: _*).orderBy(col("_change_ordinal").desc)
          session.read
            .table(temporaryViewName)
            .where(expr("_change_type IN ('INSERT','UPDATE_AFTER')"))
            .withColumn("_rank", rank().over(windowSpec))
            .where("_rank == 1")
            .drop("_rank", "_change_type", "_change_ordinal", "_commit_snapshot_id")
        }
        dataObject.incrementalOutputExpr = getCurrentSnapshotId.map(_.toString)
        icebergTable
      case _ => session.read.options(dataObject.options).table(table.fullName)
    }
    SparkConnectDataFrame(df)
  }

  override def writeDataFrame(genericDf: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    val sparkDf = genericDf match {
      case d: SparkConnectDataFrame => d
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(genericDf)
    }
    val df = sparkDf.inner
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(sparkDf)).getOrElse(sparkDf).inner
    val targetSchema = targetDf.schema

    dataObject.validateSchemaMin(SparkConnectSchema(targetSchema), "write")
    dataObject.validateSchemaHasPartitionCols(targetDf.columns.toIndexedSeq, "write")
    dataObject.validateSchemaHasPrimaryKeyCols(targetDf.columns.toIndexedSeq, "write")

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(dataObject.saveMode)

    // remember previous snapshot id to detect if data was written
    val previousSnapshotId: Option[Long] = if (isTableExisting) getCurrentSnapshotId else None

    if (isTableExisting) {
      // check schema
      if (!dataObject.allowSchemaEvolution) dataObject.validateSchema(SparkConnectSchema(targetSchema), SparkConnectSchema(session.table(table.fullName).schema), "write")
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions.
        // Therefore, df instead of targetDf is passed on.
        SparkConnectTableUtil.mergeDataFrameByPrimaryKey(session, df, table, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()), dataObject.allowSchemaEvolution, id)
      } else if (finalSaveMode == SDLSaveMode.Overwrite && dataObject.partitions.nonEmpty && partitionValues.isEmpty) {
        // dynamic partition overwrite: overwrite the partitions contained in the DataFrame
        SparkConnectTableUtil.insertIntoDynamicPartitionOverwrite(session, targetDf, table, dataObject.options, id, dataObject.getClass.getSimpleName)
      } else {
        // Make sure write.spark.accept-any-schema is set accordingly for schema evolution, see also IcebergTableSparkClassicEngine
        updateTableProperty(acceptAnySchemaProperty, dataObject.allowSchemaEvolution.toString)
        // V2 writer supports overwriting given partitions
        val dfWriterV2 = targetDf.writeTo(table.fullName).options(dataObject.options)
          .option(mergeSchemaOption, dataObject.allowSchemaEvolution.toString)
        SparkConnectTableUtil.execV2(finalSaveMode, dfWriterV2, partitionValues)
      }
    } else {
      // create new table with the V1 writer, it is needed to define the location of an external table
      var dfWriter = targetDf.write.format("iceberg").options(dataObject.options)
      // Note: for external tables the path is resolved client-side; configure an absolute path incl. scheme to make sure it is valid on the server.
      if (dataObject.path.isDefined) dfWriter = dfWriter.option("path", dataObject.hadoopPath.toString)
      dfWriter.optionalPartitionBy(dataObject.partitions).saveAsTable(table.fullName)
    }

    // get iceberg snapshot summary / stats
    val (currentSnapshotId, summary) = getCurrentSnapshot
      .getOrElse(throw new IllegalStateException(s"($id) No Iceberg snapshot found after writing to ${table.fullName}"))
    if (previousSnapshotId.contains(currentSnapshotId)) {
      logger.info(s"($id) No new iceberg snapshot was written. No data was written to this Iceberg table.")
      throw NoDataToProcessWarning(id.id, s"($id) No data was written to Iceberg table by Spark.")
    }
    IcebergTableSparkConnectEngine.normalizeIcebergMetrics(summary, finalSaveMode)
  }

  override def vacuum()(implicit context: ActionPipelineContext): Unit = {
    dataObject.historyRetentionPeriod.foreach { hours =>
      val olderThan = new Timestamp(System.currentTimeMillis - hours * 60 * 60 * 1000)
      val (_, d) = PerformanceUtils.measureDuration {
        session.sql(s"CALL $catalogName.system.expire_snapshots(table => '$identifierName', older_than => TIMESTAMP '$olderThan')").collect()
      }
      logger.info(s"($id) vacuum took $d")
    }
  }

  // cache response to avoid remote catalog query.
  @transient private var cachedIsDbExisting: Option[Boolean] = None
  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    cachedIsDbExisting.getOrElse {
      cachedIsDbExisting = Some(Try(session.sql(s"DESCRIBE NAMESPACE ${table.getDbName}").collect()).isSuccess)
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
    session.sql(s"DROP TABLE IF EXISTS ${table.fullName} PURGE").collect()
    if (dataObject.path.isDefined) logger.warn(s"($id) the path of the external table is not deleted with the Spark Connect engine")
    cachedIsTableExisting = None
  }

  override def getTableLocation(implicit context: ActionPipelineContext): Option[String] = {
    if (isTableExisting) {
      session.sql(s"DESCRIBE TABLE EXTENDED ${table.fullName}")
        .where(col("col_name") === "Location").select("data_type").collect().headOption.map(_.getString(0))
    } else None
  }

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (isTableExisting) {
      val dfPartitions = session.table(s"${table.fullName}.partitions")
      val isPartitioned = dfPartitions.columns.contains("partition")
      if (dataObject.partitions.nonEmpty && !isPartitioned) logger.warn(s"($id) partitions are defined but Iceberg table is not partitioned.")
      if (dataObject.partitions.isEmpty && isPartitioned) logger.warn(s"($id) partitions are not defined but Iceberg table is partitioned.")
      if (dataObject.partitions.nonEmpty && isPartitioned) {
        val dfPartitionsPartition = dfPartitions.select(col("partition.*"))
        val partitionValues = dfPartitionsPartition.collect().toSeq.map(r => r.getValuesMap[Any](dfPartitionsPartition.columns.toSeq).view.mapValues(_.toString).toMap)
        partitionValues.map(PartitionValues(_))
      } else Seq()
    } else Seq()
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    SparkConnectTableUtil.deletePartitions(session, table, partitionValues)
  }

  override def getStats(update: Boolean)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      val snapshotRow = session.sql(s"SELECT snapshot_id, summary, unix_millis(committed_at) as ts FROM ${table.fullName}.snapshots ORDER BY committed_at DESC LIMIT 1").head()
      val summary = snapshotRow.getMap[String, String](1)
      val lastModifiedAt = snapshotRow.getLong(2)
      val oldestSnapshotTs = session.sql(s"SELECT unix_millis(min(made_current_at)) as ts FROM ${table.fullName}.history").head().getLong(0)
      val branches = session.sql(s"SELECT name FROM ${table.fullName}.refs WHERE type = 'BRANCH'").collect().map(_.getString(0)).mkString(",")
      val icebergStats = Map(TableStatsType.LastModifiedAt.toString -> lastModifiedAt, TableStatsType.NumRows.toString -> summary("total-records").toLong, TableStatsType.NumDataFilesCurrent.toString -> summary("total-data-files").toInt, TableStatsType.Branches.toString -> branches, TableStatsType.OldestSnapshotTs.toString -> oldestSnapshotTs)
      val columnStats = getColumnStats(update, Some(lastModifiedAt))
      // no Hadoop path stats with the Spark Connect engine (no client filesystem access to the table location)
      icebergStats ++ dataObject.getPartitionStats + (TableStatsType.Columns.toString -> columnStats)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get table stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map(TableStatsType.Info.toString -> e.getMessage)
    }
  }

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])
                             (implicit context: ActionPipelineContext): Map[String, Map[String, Any]] = {
    try {
      val filesDf = session.table(s"${table.fullName}.files")
      // note that the readable_metrics of the files metadata table are per data file, they need to be aggregated over all data files
      val columns = filesDf.select(col("readable_metrics.*")).schema.fieldNames.toSeq
      val aggregations = columns.zipWithIndex.flatMap { case (c, idx) =>
        Seq(
          sum(col(s"readable_metrics.`$c`.null_value_count")).as(s"c${idx}_nulls"),
          min(col(s"readable_metrics.`$c`.lower_bound")).as(s"c${idx}_min"),
          max(col(s"readable_metrics.`$c`.upper_bound")).as(s"c${idx}_max")
        )
      }
      val statsRow = filesDf.agg(aggregations.head, aggregations.tail: _*).head()
      columns.zipWithIndex.map { case (c, idx) =>
        c -> Map(
          ColumnStatsType.NullCount.toString -> statsRow.getAs[Any](s"c${idx}_nulls"),
          ColumnStatsType.Min.toString -> statsRow.getAs[Any](s"c${idx}_min"),
          ColumnStatsType.Max.toString -> statsRow.getAs[Any](s"c${idx}_max")
        )
      }.toMap
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get column stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map()
    }
  }

  override def sql(stmt: String)(implicit context: ActionPipelineContext): GenericDataFrame = {
    SparkConnectDataFrame(session.sql(stmt))
  }

  /** current snapshot id and summary read from the Iceberg snapshots metadata table */
  private def getCurrentSnapshot(implicit context: ActionPipelineContext): Option[(Long, scala.collection.Map[String, String])] = {
    val rows: Array[Row] = session.sql(s"SELECT snapshot_id, summary FROM ${table.fullName}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    rows.headOption.map(r => (r.getLong(0), r.getMap[String, String](1)))
  }

  private def getCurrentSnapshotId(implicit context: ActionPipelineContext): Option[Long] = getCurrentSnapshot.map(_._1)
}

private[smartdatalake] object IcebergTableSparkConnectEngine {

  /**
   * Normalize an Iceberg snapshot summary to standard SDLB metric names.
   * Note that this is the same normalization as done by the classic Spark engine.
   */
  def normalizeIcebergMetrics(summary: scala.collection.Map[String, String], finalSaveMode: SDLSaveMode): MetricsMap = {
    summary.filter(_._1 != "spark.app.id")
      // normalize names lowercase with underscore
      .map { case (k, v) => (k.replace("-", "_"), Try(v.toLong).getOrElse(v)) }
      // standardize naming
      // Unfortunately this is not possible yet for merge operation, as we only get added/deleted records. Added records contain inserted + updated rows, deleted records probably updated + deleted rows...
      .map {
        case ("added_records", v) if finalSaveMode != SDLSaveMode.Merge => ("rows_inserted", v)
        case (k, v) => (k, v)
      }.toMap
  }
}
