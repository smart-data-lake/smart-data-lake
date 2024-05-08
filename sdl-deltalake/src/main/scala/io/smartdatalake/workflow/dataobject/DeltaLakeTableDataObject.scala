/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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

import com.typesafe.config.Config
import io.delta.tables.DeltaTable
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions._
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues, UCFileSystemFactory}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.{AclDef, AclUtil, PerformanceUtils, ProductUtil}
import io.smartdatalake.util.spark.{DataFrameUtil, SparkQueryUtil}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.DeltaLakeTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkColumn, SparkDataFrame, SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/**
 * [[DataObject]] of type DeltaLakeTableDataObject.
 * Provides details to access Tables in delta format to an Action.
 *
 * Delta format maintains a transaction log in a separate _delta_log subfolder.
 * The schema is registered in Metastore by DeltaLakeTableDataObject.
 *
 * The following anomalies might occur:
 * - table is registered in metastore but path does not exist -> table is dropped from metastore
 * - table is registered in metastore but path is empty -> error is thrown. Delete the path to clean up
 * - table is registered and path contains parquet files, but _delta_log subfolder is missing -> path is converted to delta format
 * - table is not registered but path contains parquet files and _delta_log subfolder -> Table is registered
 * - table is not registered but path contains parquet files without _delta_log subfolder -> path is converted to delta format and table is registered
 * - table is not registered and path does not exists -> table is created on write
 *
 *  * DeltaLakeTableDataObject implements
 * - [[CanMergeDataFrame]] by using DeltaTable.merge API.
 * - [[CanEvolveSchema]] by using mergeSchema option.
 * - Overwriting partitions is implemented by replaceWhere option in one transaction.
 *
 * @param id unique name of this data object
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 * @param partitions partition columns for this data object
 * @param options Options for Delta Lake tables see: [[https://docs.delta.io/latest/delta-batch.html]] and [[org.apache.spark.sql.delta.DeltaOptions]]
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param table DeltaLake table to be written by this output
 * @param constraints List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param preReadSql SQL-statement to be executed in exec phase before reading input table. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param postReadSql SQL-statement to be executed in exec phase after reading input table and before action is finished. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param preWriteSql SQL-statement to be executed in exec phase before writing output table. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param postWriteSql SQL-statement to be executed in exec phase after writing output table. If the catalog and/or schema are not
 *                   explicitly defined, the ones present in the configured "table" object are used.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param saveMode [[SDLSaveMode]] to use when writing files, default is "overwrite". Overwrite, Append and Merge are supported for now.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param retentionPeriod Optional delta lake retention threshold in hours. Files required by the table for reading versions younger than retentionPeriod will be preserved and the rest of them will be deleted.
 * @param acl override connection permissions for files created tables hadoop directory with this connection
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param metadata meta data
 */
case class DeltaLakeTableDataObject(override val id: DataObjectId,
                                    path: Option[String],
                                    override val partitions: Seq[String] = Seq(),
                                    override val options: Map[String,String] = Map(),
                                    override val schemaMin: Option[GenericSchema] = None,
                                    override var table: Table,
                                    override val constraints: Seq[Constraint] = Seq(),
                                    override val expectations: Seq[Expectation] = Seq(),
                                    override val preReadSql: Option[String] = None,
                                    override val postReadSql: Option[String] = None,
                                    override val preWriteSql: Option[String] = None,
                                    override val postWriteSql: Option[String] = None,
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    override val allowSchemaEvolution: Boolean = false,
                                    retentionPeriod: Option[Int] = None, // hours
                                    acl: Option[AclDef] = None,
                                    connectionId: Option[ConnectionId] = None,
                                    override val expectedPartitionsCondition: Option[String] = None,
                                    override val housekeepingMode: Option[HousekeepingMode] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanMergeDataFrame with CanEvolveSchema with CanHandlePartitions with HasHadoopStandardFilestore with ExpectationValidation with CanCreateIncrementalOutput {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[DeltaLakeTableConnection](c))

  // prepare final path and table
  @transient private var hadoopPathHolder: Path = _

  val filetype: String = ".parquet"

  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    implicit val session: SparkSession = context.sparkSession
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"($id) DeltaTable ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = {
        if (thisIsTableExisting) new Path(getDetails.head().getAs[String]("location"))
        else getAbsolutePath
      }

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if (thisIsTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HiveUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HiveUtil.normalizePath(getAbsolutePath.toString)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"($id) Table ${table.fullName} exists already with different path ${hadoopPathHolder}. New path definition ${getAbsolutePath} is ignored!")
      }
    }
    hadoopPathHolder
  }

  private def getAbsolutePath(implicit context: ActionPipelineContext) = {
    val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
    HdfsUtil.makeAbsolutePath(prefixedPath)(getFilesystem(prefixedPath, context.serializableHadoopConf)) // dont use "filesystem" to avoid loop
  }

  table = table.overrideCatalogAndDb(connection.flatMap(_.catalog), connection.map(_.db))
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")
  }

  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.Merge).contains(saveMode), s"($id) Only saveMode Overwrite and Append supported for now.")

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    if (connection.exists(_.checkDeltaLakeSparkOptions) && !UCFileSystemFactory.isDatabricksEnv) { // check not needed if on Databricks UC environment (and actionally it fails because this is configured differently on Databricks)
      require(session.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(',')).contains("io.delta.sql.DeltaSparkSessionExtension"),
        s"($id) DeltaLake spark properties are missing. Please set spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension and spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
    }
    require(isDbExisting, s"($id) DB ${table.getDbName} doesn't exist (needs to be created manually).")
    if (!isTableExisting) {
      require(path.isDefined, s"($id) If DeltaLake table does not exist yet, path must be set.")
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
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  /**
   * converts an existing path with parquet files to delta format
   */
  private[smartdatalake] def convertPathToDeltaFormat(implicit context: ActionPipelineContext): Unit = {
    val deltaPath = s"parquet.`$hadoopPath`"
    if (partitions.isEmpty) {
      DeltaTable.convertToDelta(context.sparkSession, deltaPath)
    } else {
      val partitionSchema = StructType(partitions.map(p => StructField(p, StringType)))
      DeltaTable.convertToDelta(context.sparkSession, deltaPath, partitionSchema)
    }
  }

  private def activateCdc()(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    if(!propertyExists(enableCdcFeedProperty) && isTableExisting) HiveUtil.alterTableProperties(table, Map(enableCdcFeedProperty -> "true"))
  }

  private def propertyExists(name: String)(implicit session: SparkSession): Boolean = {
    val details = DeltaTable.forName(session, table.fullName).detail()
    val properties = details.select("properties").head.getMap[String, String](0)

    properties.contains(name)
  }

  private def propertyExistsWithValue(name: String, value: String) (implicit session: SparkSession): Boolean = {
    val details = DeltaTable.forName(session, table.fullName).detail()
    val properties = details.select("properties").head.getMap[String, String](0)

    properties.exists(_ == name -> value)
  }

  @transient private val enableCdcFeedProperty = "delta.enableChangeDataFeed"

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {

    implicit val session: SparkSession = context.sparkSession

    val cdcActivated = propertyExistsWithValue(enableCdcFeedProperty, "true")

    val df = if(cdcActivated && incrementalOutputExpr.isDefined) {

      require(table.primaryKey.isDefined, s"($id) PrimaryKey for table [${table.fullName}] needs to be defined when using DataObjectStateIncrementalMode")

      val windowSpec = Window.partitionBy(table.primaryKey.get.map(col): _*).orderBy(col("_commit_timestamp").desc)

      context.sparkSession.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", incrementalOutputExpr.get)
        .table(table.fullName)
        .where(expr("_change_type IN ('insert','update_postimage')"))
        .withColumn("_rank", rank().over(windowSpec))
        .where("_rank == 1")
        .drop("_rank", "_change_type", "_commit_version", "_commit_timestamp")

    } else
      context.sparkSession.table(table.fullName)

    if(!propertyExists(enableCdcFeedProperty) && incrementalOutputExpr.isDefined) activateCdc()

    validateSchemaMin(SparkSchema(df.schema), "read")
    validateSchemaHasPartitionCols(df, "read")
    df


  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    validateSchemaHasPrimaryKeyCols(df, table.primaryKey.getOrElse(Seq()), "write")

  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists(a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined, s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    validateSchemaHasPrimaryKeyCols(df, table.primaryKey.getOrElse(Seq()), "write")
    writeDataFrame(df, createTableOnly = false, partitionValues, saveModeOptions)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates DeltaLake table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  def writeDataFrame(df: DataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])
                    (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = context.sparkSession
    implicit val helper: SparkSubFeed.type = SparkSubFeed
    val dfPrepared = if (createTableOnly) {
      // create empty df with existing df's schema
      DataFrameUtil.getEmptyDataFrame(df.schema)
    } else df

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(dfPrepared)).getOrElse(dfPrepared)
    val userMetadata = s"${context.application} runId=${context.executionId.runId} attemptId=${context.executionId.attemptId}"
    session.conf.set("spark.databricks.delta.commitInfo.userMetadata", userMetadata)
    val dfWriter = saveModeTargetDf.write
      .format("delta")
      .options(options)
      .option("path", hadoopPath.toString)
      .option("userMetadata", userMetadata)

    val sparkMetrics = if (isTableExisting) {
      if (!allowSchemaEvolution) validateSchema(SparkSchema(saveModeTargetDf.schema), SparkSchema(session.table(table.fullName).schema), "write")
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions. Therefore dfPrepared instead of saveModeTargetDf is passed on.
        mergeDataFrameByPrimaryKey(dfPrepared, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
      } else SparkStageMetricsListener.execWithMetrics(this.id, {
        if (partitions.isEmpty) {
          dfWriter
            .option("overwriteSchema", allowSchemaEvolution) // allow overwriting schema when overwriting whole table
            .option("mergeSchema", allowSchemaEvolution)
            .mode(SparkSaveMode.from(finalSaveMode))
            .save() // SaveMode append has strange errors with Table API in delta version 1.1.9
        } else {
          // insert
          if (finalSaveMode == SDLSaveMode.Overwrite) {
            val overwriteModeisDynamic = options.get("partitionOverwriteMode").contains("dynamic")
            (partitionValues.isEmpty, overwriteModeisDynamic) match {
              case (true, false) => throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data. Set option.partitionOverwriteMode=dynamic on this DeltaLakeTableDataObject to enable delta lake dynamic partitioning and get around this exception.")
              case (true, true) => { 
                dfWriter
                .option("mergeSchema", allowSchemaEvolution)
                .mode(SparkSaveMode.from(finalSaveMode))
                .save() // atomic replace (replaceWhere) doesn't work with Table API
              }
              case _ => {
                dfWriter
                .option("replaceWhere", partitionValues.map(_.getFilterExpr).reduce(_ or _).exprSql)
                .option("mergeSchema", allowSchemaEvolution)
                .mode(SparkSaveMode.from(finalSaveMode))
                .save() // atomic replace (replaceWhere) doesn't work with Table API
              }
            }
          } else {
            dfWriter
              .mode(SparkSaveMode.from(finalSaveMode))
              .option("mergeSchema", allowSchemaEvolution)
              .save() // it seems generally more stable to work without Table API
          }
        }
      })
    } else SparkStageMetricsListener.execWithMetrics(this.id,
      dfWriter
        .partitionBy(partitions: _*)
        .saveAsTable(table.fullName)
    )

    // get delta table operational metrics
    val dfHistory = DeltaTable.forName(session, table.fullName).history(1)
    if (logger.isDebugEnabled) dfHistory.show(false)
    val latestHistoryEntry = dfHistory.select("operationMetrics", "userMetadata").head()
    assert(latestHistoryEntry.getString(1) == userMetadata, s"($id) current delta lake history entry is not written by this spark application (userMetadata should be $userMetadata). Is there someone else writing to this table?!")
    val deltaMetrics = dfHistory.select("operationMetrics").head().getMap[String,String](0)
      // normalize names lowercase with underscore
      .map{case (k,v) => (DataFrameUtil.strCamelCase2LowerCaseWithUnderscores(k), Try(v.toLong).getOrElse(v))}
      // standardize naming
      .map{
        case ("num_output_rows", v) => "rows_inserted" -> v
        case ("num_updated_rows", v) => "rows_updated" -> v
        case ("num_deleted_rows", v) => "rows_deleted" -> v
        case ("num_target_rows_inserted", v) => "rows_inserted" -> v
        case ("num_target_rows_updated", v) => "rows_updated" -> v
        case ("num_target_rows_deleted", v) => "rows_deleted" -> v
        case (k,v) => k -> v
      }

    // vacuum delta lake table
    vacuum

    // fix acls
    if (acl.isDefined) AclUtil.addACLs(acl.get, hadoopPath)(filesystem)

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
    implicit val session: SparkSession = context.sparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")

    // set schema evolution support
    // this is done in a synchronized block because DataObjects with or without autoMerge enabled can be mixed and executed in parallel in a DAG
    DeltaLakeTableDataObject.synchronized { // note that this is synchronizing on the object (singleton)
      // create missing columns to support schema evolution
      val insertCols = df.columns.diff(saveModeOptions.insertColumnsToIgnore)
      if (saveModeOptions.updateColumnsOpt.isDefined || saveModeOptions.insertColumnsToIgnore.nonEmpty || saveModeOptions.insertValuesOverride.nonEmpty) {
        val existingCols = session.table(table.fullName).schema.fieldNames
        insertCols.diff(existingCols).foreach { col =>
          val sqlType = df.schema(col).dataType.sql
          logger.info(s"($id) Manually creating col $col for working around schema evolution limitations with merge statement")
          session.sql(s"ALTER TABLE ${table.fullName} ADD COLUMN $col $sqlType")
        }
      }
      session.conf.set("spark.databricks.delta.schema.autoMerge.enabled", allowSchemaEvolution)
      val deltaTable = DeltaTable.forName(session, table.fullName).as("existing")
      // prepare join condition
      val joinCondition = table.primaryKey.get.map(colName => col(s"new.$colName") === col(s"existing.$colName")).reduce(_ and _)
      var mergeStmt = deltaTable.merge(df.as("new"), joinCondition and saveModeOptions.additionalMergePredicateExpr.getOrElse(lit(true)))
      // add delete clause if configured
      saveModeOptions.deleteConditionExpr.foreach(c => mergeStmt = mergeStmt.whenMatched(c).delete())
      // add update clause - updateExpr does not support referring new columns in existing table on schema evolution, that's why we use it only when needed, and updateAll otherwise
      mergeStmt = if (saveModeOptions.updateColumnsOpt.isDefined) {
        val updateCols = saveModeOptions.updateColumnsOpt.getOrElse(df.columns.toSeq.diff(table.primaryKey.get))
        mergeStmt.whenMatched(saveModeOptions.updateConditionExpr.getOrElse(lit(true))).updateExpr(updateCols.map(c => c -> s"new.$c").toMap)
      } else {
        mergeStmt.whenMatched(saveModeOptions.updateConditionExpr.getOrElse(lit(true))).updateAll()
      }
      // add insert clause - insertExpr does not support referring new columns in existing table on schema evolution, that's why we use it only when needed, and insertAll otherwise
      mergeStmt = if (saveModeOptions.insertColumnsToIgnore.nonEmpty || saveModeOptions.insertValuesOverride.nonEmpty) {
        // create merge statement
        mergeStmt.whenNotMatched(saveModeOptions.insertConditionExpr.getOrElse(lit(true)))
          .insertExpr(insertCols.map(c => c -> saveModeOptions.insertValuesOverride.getOrElse(c, s"new.$c")).toMap)
      } else {
        mergeStmt.whenNotMatched(saveModeOptions.insertConditionExpr.getOrElse(lit(true))).insertAll()
      }
      logger.info(s"($id) executing merge statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptions).map(e => e._1+"="+e._2).mkString(" ")}")
      // execute delta lake statement
      SparkStageMetricsListener.execWithMetrics(this.id,
        mergeStmt.execute()
      )
    }
  }

  def vacuum(implicit context: ActionPipelineContext): Unit = {
    retentionPeriod.foreach { period =>
      val (_, d) = PerformanceUtils.measureDuration {
        DeltaTable.forPath(context.sparkSession, hadoopPath.toString).vacuum(period)
      }
      logger.info(s"($id) vacuum took $d")
    }
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    context.sparkSession.catalog.databaseExists(table.getDbName)
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    context.sparkSession.catalog.tableExists(table.fullName)
  }

  /**
   * Configure whether [[io.smartdatalake.workflow.action.Action]]s should fail if the input file(s) are missing
   * on the file system.
   *
   * Default is false.
   */
  def failIfFilesMissing: Boolean = false

  /**
   * Check if the input files exist.
   *
   * @throws IllegalArgumentException if `failIfFilesMissing` = true and no files found at `path`.
   */
  protected def checkFilesExisting(implicit context: ActionPipelineContext): Boolean = {
    val hasFiles = filesystem.exists(hadoopPath.getParent) &&
      RemoteIteratorWrapper(filesystem.listFiles(hadoopPath, true)).exists(_.getPath.getName.endsWith(filetype))
    if (!hasFiles) {
      logger.warn(s"($id) No files found at $hadoopPath. Can not import any data.")
      require(!failIfFilesMissing, s"($id) failIfFilesMissing is enabled and no files to process have been found in $hadoopPath.")
    }
    hasFiles
  }

  protected val separator: Char = Path.SEPARATOR_CHAR

  /**
   * List partitions.
   * Note that we need a Spark SQL statement as there might be partition directories with no current data inside
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    val (pvs,d) = PerformanceUtils.measureDuration(
      if(isTableExisting) PartitionValues.fromDataFrame(SparkDataFrame(context.sparkSession.table(table.fullName).select(partitions.map(col):_*).distinct()))
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
    val deltaTable = DeltaTable.forName(context.sparkSession, table.fullName)
    partitionValues.map(_.getFilterExpr).foreach(expr => deltaTable.delete(expr.exprSql))
  }

  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    val deltaTable = DeltaTable.forName(context.sparkSession, table.fullName)
    partitionValues.foreach {
      case (pvExisting, pvNew) =>
        deltaTable.update(pvExisting.getFilterExpr(SparkSubFeed).asInstanceOf[SparkColumn].inner, pvNew.elements.mapValues(lit).toMap)
        logger.info(s"($id) Partition $pvExisting moved to $pvNew")
    }
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    HiveUtil.dropTable(table, hadoopPath, doPurge = false)
  }

  def getDetails(implicit session: SparkSession): DataFrame = {
    DeltaTable.forName(session, table.fullName).detail()
  }

  override def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      implicit val session = context.sparkSession
      import session.implicits._
      val dfHistory = DeltaTable.forName(session, table.fullName).history()
        .select("timestamp", "userMetadata").as[(Long,String)]
      val (_,lastCommitMsg) = dfHistory.head()
      val (oldestSnapshot,_) = dfHistory.head()
      val (createdAt, lastModifiedAt, numDataFilesCurrent, sizeInBytesCurrent, properties) = getDetails
        .select("createdAt","lastModified","numFiles","sizeInBytes","properties").as[(Long,Long,Long,Long,Map[String,String])].head()
      val numRows = DeltaTable.forName(session, table.fullName).toDF.count() // This is actionally calculated by Metadata only :-)
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
      val session = context.sparkSession
      import session.implicits._
      val deltaLog = DeltaLog.forTable(session, table.tableIdentifier)
      val snapshot = deltaLog.unsafeVolatileSnapshot
      val columns = snapshot.schema.fieldNames
      def getAgg(col: String) = struct(
        min($"stats.minValues"(col)).as("minValue"),
        max($"stats.maxValues"(col)).as("maxValue"),
        sum($"stats.nullCount"(col)).as("nullCount")
      ).as(col)
      val metricsRow = snapshot.allFiles
        .select(from_json($"stats", snapshot.statsSchema).as("stats"))
        .agg(sum($"stats.numRecords").as("numRecords"), columns.map(getAgg):_*).head()
      columns.map {
        c =>
          val struct = metricsRow.getStruct(metricsRow.fieldIndex(c))
          c -> Map(
            ColumnStatsType.NullCount.toString -> struct.getAs[Long]("nullCount"),
            ColumnStatsType.Min.toString -> struct.getAs[Any]("minValue"),
            ColumnStatsType.Max.toString -> struct.getAs[Any]("maxValue")
          )
      }.toMap
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get column stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map()
    }

  }

  override def factory: FromConfigFactory[DataObject] = DeltaLakeTableDataObject


  private var incrementalOutputExpr: Option[String] = None

  /**
   * To implement incremental processing this function is called to initialize the DataObject with its state from the last increment.
   * The state is just a string. It's semantics is internal to the DataObject.
   * Note that this method is called on initializiation of the SmartDataLakeBuilder job (init Phase) and for streaming execution after every execution of an Action involving this DataObject (postExec).
   *
   * @param state Internal state of last increment. If None then the first increment (may be a full increment) is delivered.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {

    incrementalOutputExpr = state

  }

  /**
   * Return the last table version
   */
  override def getState: Option[String] = {

    val dfHistory = DeltaTable.forName(table.fullName).history(1)
    val latestVersion = String.valueOf(dfHistory.select("version").head.get(0))

    Option(latestVersion)
  }

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    sqlOpt.foreach( stmt => SparkQueryUtil.executeSqlStatementBasedOnTable(session, stmt, table))
  }
}

object DeltaLakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeltaLakeTableDataObject = {
    extract[DeltaLakeTableDataObject](config)
  }
}

