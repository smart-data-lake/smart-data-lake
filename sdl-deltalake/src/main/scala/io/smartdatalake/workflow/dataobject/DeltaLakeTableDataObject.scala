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
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.{AclDef, AclUtil, PerformanceUtils}
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.connection.DeltaLakeTableConnection
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
 * @param id unique name of this data object
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 * @param partitions partition columns for this data object
 * @param options Options for Delta Lake tables see: [[https://docs.delta.io/latest/delta-batch.html]] and [[org.apache.spark.sql.delta.DeltaOptions]]
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param table DeltaLake table to be written by this output
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
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    override val allowSchemaEvolution: Boolean = false,
                                    retentionPeriod: Option[Int] = None, // hours
                                    acl: Option[AclDef] = None,
                                    connectionId: Option[ConnectionId] = None,
                                    override val expectedPartitionsCondition: Option[String] = None,
                                    override val housekeepingMode: Option[HousekeepingMode] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with CanMergeDataFrame with CanEvolveSchema with CanHandlePartitions with HasHadoopStandardFilestore {

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
        if (thisIsTableExisting) new Path(session.sql(s"DESCRIBE DETAIL ${table.fullName}").head.getAs[String]("location"))
        else getAbsolutePath
      }

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if (thisIsTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HiveUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HiveUtil.normalizePath(getAbsolutePath.toString)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"($id) Table ${table.fullName} exists already with different path. The table will use the existing path definition $hadoopPathHolder!")
      }
    }
    hadoopPathHolder
  }

  private def getAbsolutePath(implicit context: ActionPipelineContext) = {
    val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
    HdfsUtil.makeAbsolutePath(prefixedPath)(getFilesystem(prefixedPath, context.serializableHadoopConf)) // dont use "filesystem" to avoid loop
  }

  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")
  }

  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.Merge).contains(saveMode), s"($id) Only saveMode Overwrite and Append supported for now.")

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    if (connection.exists(_.checkDeltaLakeSparkOptions)) {
      require(session.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(',')).contains("io.delta.sql.DeltaSparkSessionExtension"),
        s"($id) DeltaLake spark properties are missing. Please set spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension and spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
    }
    require(isDbExisting, s"($id) DB ${table.db.get} doesn't exist (needs to be created manually).")
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

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    val df = context.sparkSession.table(table.fullName)
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
                             (implicit context: ActionPipelineContext): Unit = {
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
                    (implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    implicit val helper: SparkSubFeed.type = SparkSubFeed
    val dfPrepared = if (createTableOnly) {
      // create empty df with existing df's schema
      DataFrameUtil.getEmptyDataFrame(df.schema)
    } else df

    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(dfPrepared)).getOrElse(dfPrepared)
    val dfWriter = saveModeTargetDf.write
      .format("delta")
      .options(options)
      .option("path", hadoopPath.toString)

    if (isTableExisting) {
      if (!allowSchemaEvolution) validateSchema(SparkSchema(saveModeTargetDf.schema), SparkSchema(session.table(table.fullName).schema), "write")
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions. Therefore dfPrepared instead of saveModeTargetDf is passed on.
        mergeDataFrameByPrimaryKey(dfPrepared, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
      } else {
        if (partitions.isEmpty) {
          dfWriter
            .option("overwriteSchema", allowSchemaEvolution) // allow overwriting schema when overwriting whole table
            .option("mergeSchema", allowSchemaEvolution)
            .mode(SparkSaveMode.from(finalSaveMode))
            .save() // SaveMode append has strange errors with Table API in delta version 1.1.9
        } else {
          // insert
          if (finalSaveMode == SDLSaveMode.Overwrite) {
            if (partitionValues.isEmpty) throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
            dfWriter
              .option("replaceWhere", partitionValues.map(_.getFilterExpr).reduce(_ or _).exprSql)
              .option("mergeSchema", allowSchemaEvolution)
              .mode(SparkSaveMode.from(finalSaveMode))
              .save() // atomic replace (replaceWhere) doesn't work with Table API
          } else {
            dfWriter
              .mode(SparkSaveMode.from(finalSaveMode))
              .option("mergeSchema", allowSchemaEvolution)
              .save() // it seems generally more stable to work without Table API
          }
        }
      }
    } else {
      dfWriter
        .partitionBy(partitions: _*)
        .saveAsTable(table.fullName)
    }

    // vacuum delta lake table
    vacuum

    // fix acls
    if (acl.isDefined) AclUtil.addACLs(acl.get, hadoopPath)(filesystem)
  }

  /**
   * Merges DataFrame with existing table data by using DeltaLake Upsert-statement.
   *
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   *
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")

    // set schema evolution support
    // this is done in a synchronized block because DataObjects with or without autoMerge enabled can be mixed and executed in parallel in a DAG
    DeltaLakeTableDataObject.synchronized { // note that this is synchronizing on the object (singleton)
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
      mergeStmt = if (saveModeOptions.insertColumnsToIgnore.nonEmpty) {
        val insertCols = df.columns.diff(saveModeOptions.insertColumnsToIgnore)
        mergeStmt.whenNotMatched(saveModeOptions.insertConditionExpr.getOrElse(lit(true))).insertExpr(insertCols.map(c => c -> s"new.$c").toMap)
      } else {
        mergeStmt.whenNotMatched(saveModeOptions.insertConditionExpr.getOrElse(lit(true))).insertAll()
      }
      // execute delta lake statement
      mergeStmt.execute()
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
    context.sparkSession.catalog.databaseExists(table.db.get)
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
      if(isTableExisting) PartitionValues.fromDataFrame(context.sparkSession.table(table.fullName).select(partitions.map(col):_*).distinct)
      else Seq()
    )
    logger.debug(s"($id) listPartitions took $d")
    // return
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

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    HiveUtil.dropTable(table, hadoopPath, doPurge = false)
  }

  override def factory: FromConfigFactory[DataObject] = DeltaLakeTableDataObject

}

object DeltaLakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeltaLakeTableDataObject = {
    extract[DeltaLakeTableDataObject](config)
  }
}

