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

import io.delta.tables.DeltaTable
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues, UCFileSystemFactory}
import io.smartdatalake.util.historization.Historization
import io.smartdatalake.util.misc.{PerformanceUtils, ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.dataset.ReadWrite
import io.smartdatalake.util.spark.{SparkSQLUtil, SparkSchemaUtil, SparkStageMetricsListener}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.dataframe.{GenericColumn, GenericDataFrame}
import io.smartdatalake.workflow.dataobject.spark.SparkSaveMode
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ProcessingLogicException}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import java.sql.Timestamp
import java.time.{Duration, LocalDateTime}
import scala.language.implicitConversions
import scala.util.Try

import scala.reflect.runtime.universe.{Type, typeOf}

/**
 * Classic Spark engine implementation of [[DeltaLakeTableDataObject]], using the DeltaTable/DeltaLog API.
 * Discovered on the classpath by [[io.smartdatalake.workflow.dataobject.generic.DataObjectEngine]].
 */
class DeltaLakeTableSparkClassicEngine(dataObject: DeltaLakeTableDataObject) extends DeltaLakeTableEngine
  with ReadWrite with SmartDataLakeLogger {
  import dataObject._

  override val subFeedType: Type = typeOf[SparkSubFeed]

  private def deltaTable(implicit session: SparkSession): DeltaTable = DeltaTable.forName(session, table.fullName)

  override def prepare()(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    if (connection.exists(_.checkDeltaLakeSparkOptions) && !UCFileSystemFactory.isDatabricksEnv) {
      // check not needed if on Databricks UC environment
      // (and actually it fails because this is configured differently on Databricks)
      require(session.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(',')).contains("io.delta.sql.DeltaSparkSessionExtension"),
        s"($id) DeltaLake spark properties are missing. Please set spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension and spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
    }
    // initialize external table if needed
    if (path.isDefined) { // if path is not defined, it is handled as managed table.
      if (!isTableExisting) {
        if (filesystem.exists(hadoopPath)) {
          if (DeltaTable.isDeltaTable(session, hadoopPath.toString)) {
            // define a delta table, metadata can be read from files.
            DeltaTable.create(session).tableName(table.fullName).location(hadoopPath.toString).execute()
            logger.info(s"($id) Creating delta table ${table.fullName} for existing path $hadoopPath")
          } else {
            // if path has existing parquet files, convert to delta table
            require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no parquet files. Delete whole base path to reset delta table.")
            convertPathToDeltaFormat
            DeltaTable.create(session).tableName(table.fullName).location(hadoopPath.toString).execute()
          }
        }
      } else if (filesystem.exists(hadoopPath)) {
        if (!DeltaTable.isDeltaTable(session, hadoopPath.toString)) {
          // if path has existing parquet files but not in delta format, convert to delta format
          require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no parquet files. Delete whole base path to reset delta table.")
          convertPathToDeltaFormat
          logger.info(s"($id) Converted existing path $hadoopPath to delta table ${table.fullName}")
        }
      } else {
        dropTable
        logger.info(s"($id) Dropped existing delta table ${table.fullName} because path was missing")
      }
    }
  }

  /**
   * converts an existing path with parquet files to delta format
   */
  private[smartdatalake] def convertPathToDeltaFormat(implicit context: ActionPipelineContext): Unit = {
    val deltaPath = s"parquet.`$hadoopPath`"
    if (partitions.isEmpty) {
      DeltaTable.convertToDelta(SparkSubFeed.getSparkSession, deltaPath)
    } else {
      val partitionSchema = StructType(partitions.map(p => StructField(p, StringType)))
      DeltaTable.convertToDelta(SparkSubFeed.getSparkSession, deltaPath, partitionSchema)
    }
  }

  private def activateCdc()(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    if(!propertyExists(enableCdcFeedProperty) && isTableExisting) SparkSQLUtil.alterTableProperties(table, Map(enableCdcFeedProperty -> "true"))
  }

  private def propertyExists(name: String)(implicit session: SparkSession): Boolean = {
    val details = deltaTable.detail()
    val properties = details.select("properties").head().getMap[String, String](0)

    properties.contains(name)
  }

  private def propertyExistsWithValue(name: String, value: String) (implicit session: SparkSession): Boolean = {
    val details = deltaTable.detail()
    val properties = details.select("properties").head().getMap[String, String](0)

    properties.exists(_ == name -> value)
  }

  @transient private val enableCdcFeedProperty = "delta.enableChangeDataFeed"

  override def getDataFrame(partitionValues: Seq[PartitionValues], incrementalOutputExpr: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame = {

    implicit val session: SparkSession = SparkSubFeed.getSparkSession

    val cdcActivated = propertyExistsWithValue(enableCdcFeedProperty, "true")

    val df = if (incrementalOutputExpr.isDefined) {

      require(table.primaryKey.isDefined, s"($id) PrimaryKey for table [${table.fullName}] needs to be defined when using DataObjectStateIncrementalMode")

      val df = if (cdcActivated && !incrementalOutputExpr.contains("0") ) {

        val windowSpec = Window.partitionBy(table.primaryKey.get.map(col).toIndexedSeq: _*).orderBy(col("_commit_timestamp").desc)

        SparkSubFeed.getSparkSession.read.format("delta")
          .option("readChangeFeed", "true")
          .option("startingVersion", incrementalOutputExpr.get)
          .table(table.fullName)
          .where(col("_change_type").isin("insert", "update_postimage"))
          .withColumn("_rank", rank().over(windowSpec))
          .where(col("_rank") === 1)
          .drop("_rank", "_change_type", "_commit_version", "_commit_timestamp")
      } else {
        if (!cdcActivated) activateCdc()
        SparkSubFeed.getSparkSession.read.options(dataObject.options).table(table.fullName)
      }

      dataObject.incrementalOutputExpr = getLatestVersion.map(v => (v + 1).toString) // version to read from next time
      logger.info(s"($id) incrementalOutputExpr=" + dataObject.incrementalOutputExpr)

      df
    } else {
      SparkSubFeed.getSparkSession.read.options(dataObject.options).table(table.fullName)
    }

    SparkDataFrame(df)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates DeltaLake table.
   */
  override def writeDataFrame(genericDf: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])
                             (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    implicit val helper: SparkSubFeed.type = SparkSubFeed

    val sparkDf = genericDf match {
      case d: SparkDataFrame => d
      case _ => DataFrameSubFeed.throwIllegalSubFeedTypeException(genericDf)
    }
    val df = sparkDf.inner
    val targetDfIncoming = saveModeOptions.map(_.convertToTargetSchema(sparkDf)).getOrElse(sparkDf).inner
    val targetSchema = targetDfIncoming.schema

    val targetDf = if (schemaMin.isDefined) {
      validateSchemaMin(SparkSchema(targetSchema), "write") //needed for merging the schemas
      val sparkSchemaMin = schemaMin.get.asInstanceOf[SparkSchema] //writeDataFrame is only done with SparkSubFeeds
      val targetSchemaWithMetadata: StructType = SparkSchemaUtil.mergeSchemaMetadata(sparkSchemaMin.inner, targetSchema)
      targetDfIncoming.to(targetSchemaWithMetadata)
    } else targetDfIncoming

    validateSchemaHasPartitionCols(targetDf.columns.toIndexedSeq, "write")
    validateSchemaHasPrimaryKeyCols(targetDf.columns.toIndexedSeq, "write")

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)

    val userMetadata = s"${context.application} runId=${context.executionId.runId} attemptId=${context.executionId.attemptId}"
    session.conf.set("spark.databricks.delta.commitInfo.userMetadata", userMetadata)
    val dfWriter = targetDf.write
      .format("delta")
      .options(options)
      .conditionalOption("path", path.isDefined, () => hadoopPath.toString) // evaluate hadoopPath only for external tables
      .option("userMetadata", userMetadata)
      .option("mergeSchema", allowSchemaEvolution) // allow schema evolution for SaveMode.Append

    val sparkMetrics = if (isTableExisting) {
      if (!allowSchemaEvolution) validateSchema(SparkSchema(targetDf.schema), SparkSchema(session.table(table.fullName).schema), "write")
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions.
        // Therefore, dfPrepared instead of saveModeTargetDf is passed on.
        mergeDataFrameByPrimaryKey(df, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
      } else SparkStageMetricsListener.execWithMetrics(id, {
        if (partitions.isEmpty) {
          // overwrite all
          dfWriter
            .option("overwriteSchema", allowSchemaEvolution) // allow overwriting schema when overwriting whole table
            .mode(SparkSaveMode.from(finalSaveMode))
            .saveAsTable(table.fullName)
        } else {
          if (finalSaveMode == SDLSaveMode.Overwrite) {
            // insert overwrite
            val overwriteModeIsDynamic = options.get("partitionOverwriteMode").orElse(session.conf.getOption("spark.sql.sources.partitionOverwriteMode")).contains("dynamic")
            if (partitionValues.isEmpty && !overwriteModeIsDynamic) throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data. Set option.partitionOverwriteMode=dynamic on this DeltaLakeTableDataObject to enable delta lake dynamic partitioning and get around this exception.")
            dfWriter
              .conditionalOption("replaceWhere", partitionValues.nonEmpty, () => partitionValues.map(_.getFilterExpr).reduce(_ or _).exprSql)
              .conditionalOption("partitionOverwriteMode", partitionValues.nonEmpty, () => "static") // reset partitionOverwriteMode=dynamic when using replaceWhere
              .mode(SparkSaveMode.from(finalSaveMode))
              .saveAsTable(table.fullName)
          } else {
            // insert append
            dfWriter
              .mode(SparkSaveMode.from(finalSaveMode))
              .saveAsTable(table.fullName)
          }
        }
      })
    } else SparkStageMetricsListener.execWithMetrics(id,
      // create new table
      dfWriter
        .optionalPartitionBy(partitions)
        .saveAsTable(table.fullName)
    )

    //if the flag is set, update comments of existing columns (one by one)
    if (updateColumnComments) {
      val columnsToUpdate = SparkSchemaUtil.identifyMissingComments(targetDf.schema, session.table(table.fullName).schema).map(kv => (kv._1.mkString("."), kv._2))
      updateExistingColumnComments(columnsToUpdate)
    }

    // get delta table operational metrics
    val dfHistory = deltaTable.history(1)
    if (logger.isDebugEnabled) dfHistory.show(false)
    val latestHistoryEntry = dfHistory.select("operationMetrics", "userMetadata").head()
    if (latestHistoryEntry.getString(1) != userMetadata) {
      logger.info(s"($id) No new version was written. No data was written to this DeltaLake table.")
      throw NoDataToProcessWarning(id.id, s"($id) No data was written to DeltaLake table by Spark.")
    }
    val deltaMetrics = DeltaLakeTableDataObject.normalizeDeltaMetrics(dfHistory.select("operationMetrics").head().getMap[String,String](0))

    // return
    sparkMetrics ++ deltaMetrics
  }

  /**
   * Merges DataFrame with existing table data by using DeltaLake Upsert-statement.
   *
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   *
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")
    val tableName = table.name
    val saveModeExpr = saveModeOptions.getExpressions(SparkSubFeed.subFeedType, existingAliasReplacement = Some(tableName))
    def toSpark(expr: GenericColumn): Column = expr.asInstanceOf[SparkColumn].inner
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
      // workaround: set this globally and check same schema before (in writeDataFrame)
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
    if(saveModeOptions.updateExistingCondition.isDefined) {
      val updateCols = df.columns.toSeq.diff(Seq(Historization.historizeOperationColName)).diff(additionalCols)
      mergeStmt = mergeStmt.whenMatched(saveModeExpr.updateExistingConditionExpr.map(toSpark).getOrElse(lit(true))).update(updateCols.map(c => c -> col(s"new.$c")).toMap)
    }

    // insert clause
    if (saveModeOptions.insertColumnsToIgnore.nonEmpty || saveModeOptions.insertValuesOverride.nonEmpty) {
      // create merge statement
      mergeStmt = mergeStmt.whenNotMatched(saveModeExpr.insertConditionExpr.map(toSpark).getOrElse(lit(true)))
        .insert(insertCols.map(c => c -> saveModeOptions.insertValuesOverride.get(c).map(lit).getOrElse(col(s"new.$c"))).toMap)
    } else {
      mergeStmt = mergeStmt.whenNotMatched(saveModeExpr.insertConditionExpr.map(toSpark).getOrElse(lit(true))).insertAll()
    }

    // execute merge statement
    logger.info(s"($id) executing merge statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1+"="+e._2).mkString(" ")}")
    SparkStageMetricsListener.execWithMetrics(id,
      mergeStmt.merge()
    )
  }

  override def vacuum()(implicit context: ActionPipelineContext): Unit = {

    val session = SparkSubFeed.getSparkSession

    def intervalHasPassed(lastExecution: Timestamp): Boolean = {
      val timePassed = Duration.between(lastExecution.toLocalDateTime, LocalDateTime.now)
      timePassed.compareTo(Duration.parse(minVacuumInterval.get)) > 0 //the time passed is greater than the set minInterval
    }

    lazy val lastVacuum = deltaTable(session).history().filter(col("operation").contains("VACUUM END")).select(max("timestamp")).collect()

    //execute vacuum if either no interval is set, there has never been a vacuum operation, or the set interval has passed
    if (minVacuumInterval.isEmpty || lastVacuum.isEmpty || intervalHasPassed(lastVacuum(0).getTimestamp(0))) {
      retentionPeriod.foreach { period =>
        val (_, d) = PerformanceUtils.measureDuration {
          DeltaTable.forPath(session, hadoopPath.toString).vacuum(period)
        }
        logger.info(s"($id) vacuum took $d")
      }
    }

  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    SparkSubFeed.getSparkSession.catalog.databaseExists(table.getDbName)
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    SparkSubFeed.getSparkSession.catalog.tableExists(table.fullName)
  }

  /**
   * List partitions.
   * Note that we need a Spark SQL statement as there might be partition directories with no current data inside
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    val (pvs,d) = PerformanceUtils.measureDuration(
      if(isTableExisting) PartitionValues.fromDataFrame(SparkDataFrame(SparkSubFeed.getSparkSession.table(table.fullName).select(partitions.map(col):_*).distinct()))
      else Seq()
    )
    logger.debug(s"($id) listPartitions took $d")
    pvs
  }

  /**
   * Note that we will not delete the whole partition but just the data of the partition because delta lake keeps history
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    implicit val helper: SparkSubFeed.type = SparkSubFeed
    partitionValues.map(_.getFilterExpr).foreach(expr => deltaTable(SparkSubFeed.getSparkSession).delete(expr.exprSql))
  }

  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    partitionValues.foreach {
      case (pvExisting, pvNew) =>
        deltaTable.update(pvExisting.getFilterExpr(SparkSubFeed).asInstanceOf[SparkColumn].inner, pvNew.elements.view.mapValues(lit).toMap)
        logger.info(s"($id) Partition $pvExisting moved to $pvNew")
    }
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    SparkSQLUtil.dropTableOptionalPath(table, if (path.isDefined) Some(hadoopPath) else None, doPurge = false)
  }

  private def getDetails(implicit session: SparkSession): DataFrame = {
    deltaTable.detail()
  }

  override def getTableLocation(implicit context: ActionPipelineContext): String = {
    implicit val session: SparkSession = SparkSubFeed.getSparkSession
    getDetails.head().getAs[String]("location")
  }

  override def getStats(update: Boolean)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      implicit val session: SparkSession = SparkSubFeed.getSparkSession
      import session.implicits._
      val dfHistory = deltaTable.history()
        .select("timestamp", "userMetadata").as[(Long,String)]
      val (_,lastCommitMsg) = dfHistory.head()
      val (oldestSnapshot,_) = dfHistory.head()
      val (createdAt, lastModifiedAt, numDataFilesCurrent, sizeInBytesCurrent, _) = getDetails
        .select("createdAt","lastModified","numFiles","sizeInBytes","properties").as[(Long,Long,Long,Long,Map[String,String])].head()
      val numRows = deltaTable.toDF.count() // This is actually calculated by Metadata only :-)
      val deltaStats = Map(TableStatsType.CreatedAt.toString -> createdAt, TableStatsType.LastModifiedAt.toString -> lastModifiedAt, TableStatsType.LastCommitMsg.toString -> lastCommitMsg, TableStatsType.NumDataFilesCurrent.toString -> numDataFilesCurrent, TableStatsType.SizeInBytesCurrent.toString -> sizeInBytesCurrent, TableStatsType.OldestSnapshotTs.toString -> oldestSnapshot, TableStatsType.NumRows.toString -> numRows)
      val columnStats = getColumnStats(update, Some(lastModifiedAt))
      HdfsUtil.getPathStats(hadoopPath)(filesystem) ++ deltaStats ++ getPartitionStats + (TableStatsType.Columns.toString -> columnStats)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get column stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map("info" -> e.getMessage)
    }
  }

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])(implicit context: ActionPipelineContext): Map[String, Map[String,Any]] = {
    try {
      val session = SparkSubFeed.getSparkSession
      val deltaLog = DeltaLog.forTable(session, TableIdentifier(table.name, table.db, table.catalog))
      val snapshot = deltaLog.unsafeVolatileSnapshot
      val columns = snapshot.schema.fieldNames
      import session.implicits._

      def colExists(schema: StructType, nestedCol: Seq[String]): Boolean = {
        nestedCol match {
          case c :: Nil => schema.fieldNames.contains(c)
          case c :: tail => schema.find(_.name == c)
            .map(_.dataType).collect { case dataType: StructType => colExists(dataType, tail) }
            .getOrElse(false)
        }
      }

      def statsColIfExists(statsCol: String, dataCol: String): Option[Column] = {
        val exists = colExists(snapshot.statsSchema, Seq(statsCol, dataCol))
        if (exists) Some($"stats"(statsCol)(dataCol))
        else None
      }

      def getAgg(col: String): Column = struct(
        Seq(
          statsColIfExists("minValues", col).map(min(_).as("minValue")),
          statsColIfExists("maxValues", col).map(max(_).as("maxValue")),
          statsColIfExists("nullCount", col).map(sum(_).as("nullCount"))
        ).flatten.toIndexedSeq: _*
      ).as(col)
      val metricsRow = snapshot.allFiles
        .select(from_json($"stats", snapshot.statsSchema).as("stats"))
        .agg(sum($"stats.numRecords").as("numRecords"), columns.map(getAgg).toIndexedSeq:_*).head()

      def getAsOption[T](row: Row, col: String): Option[T] = {
        if (row.schema.fieldNames.contains(col) && !row.isNullAt(row.fieldIndex(col))) Some(row.getAs[T](col))
        else None
      }
      columns.map {
        c =>
          val struct = metricsRow.getStruct(metricsRow.fieldIndex(c))
          c -> Seq(
            getAsOption[Long](struct, "nullCount").map(ColumnStatsType.NullCount.toString -> _),
            getAsOption[Any](struct, "minValue").map(ColumnStatsType.Min.toString -> _),
            getAsOption[Any](struct, "maxValue").map(ColumnStatsType.Max.toString -> _)
          ).flatten.toMap
      }.toMap
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get column stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map()
    }

  }

  /**
   * Return the last table version
   */
  def getLatestVersion: Option[Long] = {

    val dfHistory = DeltaTable.forName(table.fullName).history(1)
    val latestVersion = dfHistory.select("version").head().getAs[Long](0)

    Option(latestVersion)
  }

  override def sql(stmt: String)(implicit context: ActionPipelineContext): GenericDataFrame = {
    SparkDataFrame(SparkSubFeed.getSparkSession.sql(stmt))
  }
}
