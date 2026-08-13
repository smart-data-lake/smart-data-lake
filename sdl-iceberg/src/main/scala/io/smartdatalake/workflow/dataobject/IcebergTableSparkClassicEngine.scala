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
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc.{PerformanceUtils, ProductUtil, SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.SparkStageMetricsListener
import io.smartdatalake.util.spark.dataset.{Quality, ReadWrite}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.iceberg
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.spark.Spark3Util.{CatalogAndIdentifier, identifierToTableIdentifier}
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.{Spark3Util, SparkSchemaUtil, SparkWriteOptions}
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.{CachingCatalog, PartitionSpec, TableProperties}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, rank}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import java.lang.reflect.Field
import scala.annotation.tailrec
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.{Type, typeOf}
import scala.util.{Failure, Success, Try}

/**
 * Classic Spark engine implementation of [[IcebergTableDataObject]], using the Iceberg Java API,
 * the Iceberg-Spark bridge (Spark3Util, SparkActions) and driver-side DSv2 catalogs.
 * Discovered on the classpath by [[io.smartdatalake.workflow.dataobject.generic.DataObjectEngine]].
 */
class IcebergTableSparkClassicEngine(dataObject: IcebergTableDataObject) extends IcebergTableEngine
  with ReadWrite with Quality with SmartDataLakeLogger {
  import dataObject._

  override val subFeedType: Type = typeOf[SparkSubFeed]

  override def prepare()(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    if (connection.exists(_.checkIcebergSparkOptions)) {
      require(session.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(',')).contains("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        s"($id) Iceberg spark properties are missing. Please set spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions and org.apache.iceberg.spark.SparkSessionCatalog")
    }
    val thisIsPathBasedCatalog = isPathBasedCatalog(getIcebergCatalog)
    if (thisIsPathBasedCatalog && path.nonEmpty) logger.warn(s"($id) path is ignored for path based catalogs like HadoopCatalog.")
    if (!isTableExisting) {
      require(path.isDefined || thisIsPathBasedCatalog, s"($id) If Iceberg table does not exist yet, path must be set.")
      if (filesystem.exists(hadoopPath)) {
        if (filesystem.exists(getMetadataPath)) {
          // define an iceberg table, metadata can be read from files.
          getIcebergCatalog.registerTable(getTableIdentifier, getMetadataPath.toString)
          logger.info(s"($id) Creating Iceberg table ${table.fullName} for existing path $hadoopPath")
        } else {
          // if path has existing parquet files, convert to iceberg table
          require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no data files. Delete whole base path to reset Iceberg table.")
          convertPathToIceberg
        }
      }
    } else if (filesystem.exists(hadoopPath)) {
      if (!filesystem.exists(getMetadataPath)) {
        // if path has existing parquet files but not in iceberg format, convert to iceberg format
        require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no data files. Delete whole base path to reset Iceberg table.")
        convertTableToIceberg
        logger.info(s"($id) Converted existing table ${table.fullName} to Iceberg table")
      }
    } else {
      dropTable
      logger.info(s"($id) Dropped existing Iceberg table ${table.fullName} because path was missing")
    }
  }

  /**
   * converts an existing hive table with parquet files to an iceberg table
   */
  private[smartdatalake] def convertTableToIceberg(implicit context: ActionPipelineContext): Unit = {
    SparkActions.get(SparkSubFeed.getSparkSession).migrateTable(getIdentifier.toString)
  }

  /**
   * converts an existing path with parquet files to an iceberg table
   */
  private[smartdatalake] def convertPathToIceberg(implicit context: ActionPipelineContext): Unit = {
    val dataPath = new Path(hadoopPath, "data")
    if (!filesystem.exists(dataPath)) {
      // move parquet files and partitions from table root folder to data subfolder (Iceberg standard)
      val filesToMove = filesystem.listStatus(hadoopPath)
        .filter(f => (f.isFile && f.getPath.getName.matches(filetypePattern)) || (f.isDirectory && f.getPath.getName.contains("=")))
      logger.info(s"($id) convertPathToIceberg: moving ${filesToMove.length} files to ./data subdirectory")
      filesystem.mkdirs(dataPath)
      filesToMove.foreach { f =>
        val newPath = new Path(dataPath, f.getPath.getName)
        if (!filesystem.rename(f.getPath, newPath)) throw new IllegalStateException(s"($id) Failed to rename ${f.getPath} -> $newPath")
      }
    }
    // create table
    logger.info(s"($id) convertPathToIceberg: creating iceberg table")
    // get schema using Spark. Note that this only work for parquet files.
    val sparkSchema = SparkSubFeed.getSparkSession.read.parquet(dataPath.toString).schema
    createIcebergTable(sparkSchema)
    // add files
    logger.info(s"($id) convertPathToIceberg: add_files")
    val parallelismStr = connection.flatMap(_.addFilesParallelism.map(", parallelism => " + _)).getOrElse("")
    SparkSubFeed.getSparkSession.sql(s"CALL ${getIcebergCatalog.name}.system.add_files(table => '${getIdentifier.toString}', source_table => '`parquet`.`$dataPath`'$parallelismStr)")
    // cleanup potential SDLB .schema directory
    HdfsUtil.deletePath(new Path(hadoopPath, ".schema"), doWarn = false)(filesystem)
    logger.info(s"($id) convertPathToIceberg: succeeded")
  }

  private def createIcebergTable(sparkSchema: StructType)(implicit context: ActionPipelineContext) = {
    val schema = SparkSchemaUtil.convert(sparkSchema)
    val partitionSpec = partitions.foldLeft(PartitionSpec.builderFor(schema)) {
      case (partitionSpec, colName) => partitionSpec.identity(colName)
    }.build
    getIcebergCatalog.createTable(getTableIdentifier, schema, partitionSpec, hadoopPath.toString, options.asJava)
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues], incrementalOutputExpr: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    val df = incrementalOutputExpr match {
      case Some(snapshotId) =>
        require(table.primaryKey.isDefined, s"($id) PrimaryKey for table [${table.fullName}] needs to be defined when using DataObjectStateIncrementalMode")
        val icebergTable = if (snapshotId == "0") SparkSubFeed.getSparkSession.table(table.fullName)
        else {

          // activate temporary cdc view
          SparkSubFeed.getSparkSession.sql(
            s"""CALL ${getIcebergCatalog.name}.system.create_changelog_view(table => '${getIdentifier.toString}'
               |, options => map('start-snapshot-id', '$snapshotId')
               |, compute_updates => true
               |, identifier_columns => array('${table.primaryKey.get.mkString("','")}')
               |)""".stripMargin)

          // read cdc events
          val temporaryViewName = table.name + "_changes"

          val windowSpec = Window.partitionBy(table.primaryKey.get.map(col).toIndexedSeq: _*).orderBy(col("_change_ordinal").desc)
          SparkSubFeed.getSparkSession.read
            .table(temporaryViewName)
            .where(expr("_change_type IN ('INSERT','UPDATE_AFTER')"))
            .withColumn("_rank", rank().over(windowSpec))
            .where("_rank == 1")
            .drop("_rank", "_change_type", "_change_ordinal", "_commit_snapshot_id")
        }
        dataObject.incrementalOutputExpr = Some(getIcebergTable.currentSnapshot().snapshotId().toString)
        icebergTable
      case _ => SparkSubFeed.getSparkSession.table(table.fullName)
    }
    SparkDataFrame(df)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates Iceberg table.
   */
  override def writeDataFrame(genericDf: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession

    val sparkDf = genericDf match {
      case d: SparkDataFrame => d
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(genericDf)
    }
    val df = sparkDf.inner
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(sparkDf)).getOrElse(sparkDf).inner
    val targetSchema = targetDf.schema

    validateSchemaMin(SparkSchema(targetSchema), "write")
    validateSchemaHasPartitionCols(targetDf.columns.toIndexedSeq, "write")
    validateSchemaHasPrimaryKeyCols(targetDf.columns.toIndexedSeq, "write")

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    // remember previous snapshot timestamp
    val previousSnapshotId: Option[Long] = if (isTableExisting) Option(getIcebergTable.currentSnapshot()).map(_.snapshotId()) else None
    // V1 writer is needed to create external table
    var dfWriter = targetDf.write
      .format("iceberg")
      .options(options)
    if (isPathBasedCatalog(getIcebergCatalog)) dfWriter = dfWriter.option("location", hadoopPath.toString)
    else dfWriter = dfWriter.option("path", hadoopPath.toString)
    val sparkMetrics = if (isTableExisting) {
      // check scheme
      if (!allowSchemaEvolution) validateSchema(SparkSchema(targetSchema), SparkSchema(session.table(table.fullName).schema), "write")
      // apply
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions.
        // Therefore, df instead of targetDf is passed on.
        mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()), targetSchema)
      } else SparkStageMetricsListener.execWithMetrics(dataObject.id, {
        // Make sure SPARK_WRITE_ACCEPT_ANY_SCHEMA=true for schema evolution
        updateTableProperty(TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA, allowSchemaEvolution.toString, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT.toString)
        // V2 writer can be used if table is existing, it supports overwriting given partitions
        val dfWriterV2 = targetDf
          .writeTo(table.fullName)
          .option(SparkWriteOptions.MERGE_SCHEMA, allowSchemaEvolution.toString)
        if (partitions.isEmpty) {
          SparkSaveModeUtil.execV2(finalSaveMode, dfWriterV2, partitionValues)
        } else {
          val overwriteModeIsDynamic = options.get("partitionOverwriteMode").orElse(session.conf.getOption("spark.sql.sources.partitionOverwriteMode")).contains("dynamic")
          if (finalSaveMode == SDLSaveMode.Overwrite && partitionValues.isEmpty && !overwriteModeIsDynamic) {
            throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data. Set option.partitionOverwriteMode=dynamic on this IcebergTableDataObject to enable dynamic partitioning and get around this exception.")
          }
          SparkSaveModeUtil.execV2(finalSaveMode, dfWriterV2, partitionValues, overwriteModeIsDynamic)
        }
      })
    } else SparkStageMetricsListener.execWithMetrics(dataObject.id, {
      dfWriter
        .optionalPartitionBy(partitions)
        .saveAsTable(table.fullName)
    })

    // get iceberg snapshot summary / stats
    val currentSnapshot = getIcebergTable.currentSnapshot()
    if (logger.isDebugEnabled) logger.debug(s"snapshot after write: ${currentSnapshot.toString}")
    val summary = currentSnapshot.summary().asScala
    if (previousSnapshotId.contains(currentSnapshot.snapshotId())) {
      logger.info(s"($id) No new iceberg snapshot was written. No data was written to this Iceberg table.")
      throw NoDataToProcessWarning(id.id, s"($id) No data was written to Iceberg table by Spark.")
    }
    // add all summary entries except spark application id to metrics
    val icebergMetrics = summary.filter(_._1 != "spark.app.id")
      // normalize names lowercase with underscore
      .map { case (k, v) => (k.replace("-", "_"), Try(v.toLong).getOrElse(v)) }
      // standardize naming
      // Unfortunately this is not possible yet for merge operation, as we only get added/deleted records. Added records contain inserted + updated rows, deleted records probably updated + deleted rows...
      .map {
        case ("added_records", v) if finalSaveMode != SDLSaveMode.Merge => ("rows_inserted", v)
        case (k, v) => (k, v)
      }

    // return
    sparkMetrics ++ icebergMetrics
  }

  private def writeToTempTable(df: DataFrame, identifier: TableIdentifier)(implicit context: ActionPipelineContext): Unit = {
    logger.debug(s"check whether temp-table ${tmpTable.fullName} exists")
    if (getIcebergCatalog.tableExists(identifier)) {
      logger.error(s"($id) Temporary table ${tmpTable.fullName} for merge already exists!" +
        s" There might be a potential conflict with another job. It will be replaced.")
      getIcebergCatalog.dropTable(identifier)
    }
    val icebergPath = hadoopPath.toString + "_sdltmp"
    logger.debug(s"writeToTempTable: write to temp-table ${tmpTable.fullName}. Option icebergPath = $icebergPath")
    Try(df.write.format("iceberg").option("path", icebergPath).saveAsTable(tmpTable.fullName)) match {
      case Success(_) =>
      case Failure(ex) => logger.error(s"writeToTempTable: FAILED to write to temp-table ${tmpTable.toString} !!! Option icebergPath = $icebergPath\"")
        logger.error(ex.getMessage)
        df.createdLog("df")(logger)
        throw ex
    }
  }

  /**
   * Merges DataFrame with existing table data by writing DataFrame to a temp-table and using SQL Merge-statement.
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions, targetSchema: StructType)(implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")
    val tmpTableIdentifier = TableIdentifier.of(getIdentifier.namespace :+ tmpTable.name: _*)

    try {
      // write data to temp table
      val metrics = SparkStageMetricsListener.execWithMetrics(dataObject.id,
        writeToTempTable(df, tmpTableIdentifier)
      )

      // handle schema evolution on merge because this is not yet supported in Spark <=3.5
      val existingSchema = SparkSchema(session.table(table.fullName).schema)
      if (allowSchemaEvolution && !existingSchema.equalsSchema(SparkSchema(targetSchema))) evolveTableSchema(targetSchema)
      // make sure SPARK_WRITE_ACCEPT_ANY_SCHEMA=false with SQL merge, because this is not supported in Spark 3.5. See also https://github.com/apache/iceberg/issues/9827.
      updateTableProperty(TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA, "false", TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT.toString)

      // update existing does not work with SQL merge stmt
      val updateExistingStatement = SQLUtil.createUpdateExistingStatement(table, df.columns.toSeq, tmpTable.fullName, saveModeOptions, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
      updateExistingStatement.foreach { stmt =>
        logger.info(s"($id) executing update existing statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1 + "=" + e._2).mkString(" ")}")
        SparkSubFeed.getSparkSession.sql(stmt)
      }

      // override missing columns with null value, as Iceberg needs all target columns be included in insert statement
      val targetCols = session.table(table.fullName).schema.fieldNames.toSeq
      val missingCols = targetCols.diff(df.columns.toSeq)
      val saveModeOptionsExt = saveModeOptions.copy(
        insertValuesOverride = saveModeOptions.insertValuesOverride ++ missingCols.map(_ -> "null"),
        updateColumns = if (saveModeOptions.updateColumns.isEmpty) df.columns.diff(table.primaryKey.get).toSeq else saveModeOptions.updateColumns
      )
      // prepare SQL merge statement
      // note that we pass all target cols instead of new df columns as parameter, but with customized saveModeOptionsExt
      val mergeStmt = SQLUtil.createMergeStatement(table, targetCols, tmpTable.fullName, saveModeOptionsExt, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
      // execute
      logger.info(s"($id) executing merge statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptionsExt).map(e => e._1 + "=" + e._2).mkString(" ")}")
      logger.debug(s"($id) merge statement: $mergeStmt")
      SparkSubFeed.getSparkSession.sql(mergeStmt)
      // return
      metrics
    } finally {
      // cleanup temp table
      getIcebergCatalog.dropTable(tmpTableIdentifier)
    }
  }

  def updateTableProperty(name: String, value: String, default: String)
                         (implicit context: ActionPipelineContext): Unit = {
    val currentValue = getIcebergTable.properties.asScala.getOrElse(name, default)
    if (currentValue != value) {
      getIcebergTable.updateProperties().set(name, value).commit()
    }
    logger.info(s"($id) updated Iceberg table property $name to $value")
  }

  /**
   * Iceberg has a write option 'mergeSchema' (see also SparkWriteOptions.MERGE_SCHEMA),
   * but it does not work as there is another validation before that checks the schema
   * (e.g. QueryCompilationErrors$.cannotWriteTooManyColumnsToTableError in the stack trace)
   * This code is therefore copied from SparkWriteBuilder.validateOrMergeWriteSchema:246ff
   */
  def evolveTableSchema(dsSchema: StructType)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) evolving Iceberg table schema")
    val icebergTable = getIcebergTable
    val caseSensitive = Environment.caseSensitive

    // convert the dataset schema and assign fresh ids for new fields
    val newSchema = SparkSchemaUtil.convertWithFreshIds(icebergTable.schema, dsSchema, caseSensitive)

    // update the table to get final id assignments and validate the changes
    val update = icebergTable.updateSchema().caseSensitive(caseSensitive).unionByNameWith(newSchema)
    val mergedSchema = update.apply

    // reconvert the dsSchema without assignment to use the ids assigned by UpdateSchema
    val writeSchema = SparkSchemaUtil.convert(mergedSchema, dsSchema, caseSensitive)

    TypeUtil.validateWriteSchema(mergedSchema, writeSchema, false, false)

    // if the validation passed, update the table schema
    update.commit()
  }

  override def vacuum()(implicit context: ActionPipelineContext): Unit = {
    historyRetentionPeriod.foreach { hours =>
      val (_, d) = PerformanceUtils.measureDuration {
        SparkActions.get(SparkSubFeed.getSparkSession)
          .expireSnapshots(getIcebergTable)
          .expireOlderThan(System.currentTimeMillis - hours * 60 * 60 * 1000)
          .execute
      }
      logger.info(s"($id) vacuum took $d")
    }
  }

  def getIcebergCatalog(implicit context: ActionPipelineContext): Catalog = {
    getSparkCatalog.icebergCatalog
  }

  def getSparkCatalog(implicit context: ActionPipelineContext): TableCatalog with SupportsNamespaces with HasIcebergCatalog = {
    getCatalogAndIdentifier.catalog match {
      case c: TableCatalog with HasIcebergCatalog with SupportsNamespaces => c
      case c => throw new IllegalStateException(s"($id) ${c.name}:${c.getClass.getSimpleName} is not a TableCatalog with SupportsNamespaces with HasIcebergCatalog implementation")
    }
  }

  def getIdentifier(implicit context: ActionPipelineContext): Identifier = {
    getCatalogAndIdentifier.identifier
  }

  def getTableIdentifier(implicit context: ActionPipelineContext): TableIdentifier = {
    convertToTableIdentifier(getIdentifier)
  }

  def convertToTableIdentifier(identifier: Identifier): TableIdentifier = {
    TableIdentifier.of(Namespace.of(identifier.namespace: _*), identifier.name)
  }

  private def getCatalogAndIdentifier(implicit context: ActionPipelineContext): CatalogAndIdentifier = {
    if (_catalogAndIdentifier.isEmpty) {
      _catalogAndIdentifier = Some(Spark3Util.catalogAndIdentifier(SparkSubFeed.getSparkSession, table.nameParts.asJava))
    }
    _catalogAndIdentifier.get
  }

  private var _catalogAndIdentifier: Option[CatalogAndIdentifier] = None

  def getIcebergTable(implicit context: ActionPipelineContext): iceberg.Table = {
    // Note: loadTable is cached by default in Iceberg catalog
    getIcebergCatalog.loadTable(identifierToTableIdentifier(getIdentifier))
  }

  @tailrec
  private def getHadoopCatalog(catalog: Catalog): Option[HadoopCatalog] = {
    catalog match {
      case c: HadoopCatalog => Some(c)
      case c: CachingCatalog =>
        val getWrappedCatalog: Field = c.getClass.getDeclaredField("catalog")
        getWrappedCatalog.setAccessible(true)
        getHadoopCatalog(getWrappedCatalog.get(c).asInstanceOf[Catalog])
      case _ => None
    }
  }

  private def isPathBasedCatalog(catalog: Catalog): Boolean = {
    getHadoopCatalog(catalog).isDefined
  }

  private def getHadoopCatalogDefaultPath(catalog: HadoopCatalog, tableIdentifier: TableIdentifier): String = {
    val getDefaultWarehouseLocation = catalog.getClass.getDeclaredMethod("defaultWarehouseLocation", classOf[TableIdentifier])
    getDefaultWarehouseLocation.setAccessible(true)
    getDefaultWarehouseLocation.invoke(catalog, tableIdentifier).asInstanceOf[String]
  }

  override def getTableLocation(implicit context: ActionPipelineContext): Option[String] = {
    getHadoopCatalog(getIcebergCatalog).map(c => getHadoopCatalogDefaultPath(c, getTableIdentifier))
      .orElse(if (isTableExisting) Some(getIcebergTable.location) else None)
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    if (isPathBasedCatalog(getIcebergCatalog)) {
      // for hadoop catalog only table.db is relevant, table.catalog must be omitted
      getSparkCatalog.namespaceExists(Array(table.db.get))
    } else {
      getSparkCatalog.namespaceExists(table.nameParts.init.toArray)
    }
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    getIcebergCatalog.tableExists(identifierToTableIdentifier(getIdentifier))
  }

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    if (isTableExisting) {
      val dfPartitions = SparkSubFeed.getSparkSession.table(s"${table.fullName}.partitions")
      val isPartitioned = dfPartitions.columns.contains("partition")
      if (partitions.nonEmpty && !isPartitioned) logger.warn(s"($id) partitions are defined but Iceberg table is not partitioned.")
      if (partitions.isEmpty && isPartitioned) logger.warn(s"($id) partitions are not defined but Iceberg table is partitioned.")
      if (partitions.nonEmpty && isPartitioned) {
        val dfPartitionsPartition = dfPartitions.select(col("partition.*"))
        val partitionValues = dfPartitionsPartition.collect().toSeq.map(r => r.getValuesMap[Any](dfPartitionsPartition.columns.toSeq).view.mapValues(_.toString).toMap)
        partitionValues.map(PartitionValues(_))
      } else Seq()
    } else Seq()
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    val deleteStmt = SQLUtil.createDeletePartitionStatement(table.fullName, partitionValues, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
    SparkSubFeed.getSparkSession.sql(deleteStmt)
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    getIcebergCatalog.dropTable(getTableIdentifier, true) // purge
    HdfsUtil.deletePath(hadoopPath, doWarn = false)(filesystem)
  }

  override def getStats(update: Boolean)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      val icebergTable = getIcebergTable
      val branches = icebergTable.refs().asScala.filter(_._2.isBranch).keys.toSeq.mkString(",")
      val oldestSnapshot = icebergTable.history().asScala.minBy(_.timestampMillis())
      val snapshot = icebergTable.currentSnapshot()
      val summary = snapshot.summary().asScala
      val lastModifiedAt = snapshot.timestampMillis()
      val oldestSnapshotTs = oldestSnapshot.timestampMillis()
      val icebergStats = Map(TableStatsType.LastModifiedAt.toString -> lastModifiedAt, TableStatsType.NumRows.toString -> summary("total-records").toLong, TableStatsType.NumDataFilesCurrent.toString -> summary("total-data-files").toInt, TableStatsType.Branches.toString -> branches, TableStatsType.OldestSnapshotTs.toString -> oldestSnapshotTs)
      val columnStats = getColumnStats(update, Some(lastModifiedAt))
      HdfsUtil.getPathStats(hadoopPath)(filesystem) ++ icebergStats ++ getPartitionStats + (TableStatsType.Columns.toString -> columnStats)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get table stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map(TableStatsType.Info.toString -> e.getMessage)
    }
  }

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])
                             (implicit context: ActionPipelineContext): Map[String, Map[String, Any]] = {
    try {
      val session = SparkSubFeed.getSparkSession
      import session.implicits._
      val filesDf = session.table(s"${table.fullName}.files")
      val columns = filesDf.select(col("readable_metrics.*")).schema.fieldNames.toSeq
      val aggregations = columns.zipWithIndex.flatMap { case (c, idx) =>
        Seq(
          functions.sum(col(s"readable_metrics.`$c`.null_value_count")).as(s"c${idx}_nulls"),
          functions.min(col(s"readable_metrics.`$c`.lower_bound")).as(s"c${idx}_min"),
          functions.max(col(s"readable_metrics.`$c`.upper_bound")).as(s"c${idx}_max")
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
    SparkDataFrame(SparkSubFeed.getSparkSession.sql(stmt))
  }
}
