/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.DateColumnType.DateColumnType
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions._
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.{AclDef, AclUtil, CompactionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.HiveTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.Timestamp
import scala.collection.JavaConverters._

/**
 * [[DataObject]] of type Hive.
 * Provides details to access Hive tables to an Action
 *
 * @param id unique name of this data object
 * @param table hive table to be written by this output
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 *             If DataObject is only used for reading or if the HiveTable already exist, the path can be omitted.
 *             If the HiveTable already exists but with a different path, a warning is issued
 * @param partitions partition columns for this data object
 * @param acl override connections permissions for files created tables hadoop directory with this connection
 * @param analyzeTableAfterWrite enable compute statistics after writing data (default=false)
 * @param dateColumnType type of date column
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param saveMode spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param constraints List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param numInitialHdfsPartitions number of files created when writing into an empty table (otherwise the number will be derived from the existing data)
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 * @param metadata meta data
 */
case class HiveTableDataObject(override val id: DataObjectId,
                               path: Option[String] = None,
                               override val partitions: Seq[String] = Seq(),
                               analyzeTableAfterWrite: Boolean = false,
                               dateColumnType: DateColumnType = DateColumnType.Date,
                               override val schemaMin: Option[GenericSchema] = None,
                               override var table: Table,
                               override val constraints: Seq[Constraint] = Seq(),
                               override val expectations: Seq[Expectation] = Seq(),
                               numInitialHdfsPartitions: Int = 16,
                               saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               acl: Option[AclDef] = None,
                               connectionId: Option[ConnectionId] = None,
                               override val expectedPartitionsCondition: Option[String] = None,
                               override val housekeepingMode: Option[HousekeepingMode] = None,
                               override val metadata: Option[DataObjectMetadata] = None)
                              (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject with CanCreateSparkDataFrame with CanWriteSparkDataFrame with CanHandlePartitions with HasHadoopStandardFilestore
    with ExpectationValidation with SmartDataLakeLogger {

  // Hive tables are always written in parquet format
  private val fileName = "*.parquet"

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[HiveTableConnection](c))

  override def options: Map[String, String] = Map() // override options because of conflicting definitions in CanCreateSparkDataFrame and CanWriteSparkDataFrame

  // prepare table
  table = table.overrideCatalogAndDb(None, connection.map(_.db))
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection.")

  assert(saveMode!=SDLSaveMode.OverwritePreserveDirectories, s"($id) saveMode OverwritePreserveDirectories not supported for now.")

  // prepare final path
  @transient private var hadoopPathHolder: Path = _
  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    implicit val session: SparkSession = context.sparkSession
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"($id) HiveTable ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = {
        if (thisIsTableExisting) new Path(HiveUtil.existingTableLocation(table))
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
    val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.flatMap(_.pathPrefix))
    HdfsUtil.makeAbsolutePath(prefixedPath)(getFilesystem(prefixedPath, context.serializableHadoopConf)) // dont use "filesystem" to avoid loop
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    require(isDbExisting, s"($id) Hive DB ${table.db.get} doesn't exist (needs to be created manually).")
    if (!isTableExisting)
      require(path.isDefined, s"($id) If Hive table does not exist yet, the path must be set.")
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    val df = context.sparkSession.table(s"${table.fullName}")
    validateSchemaMin(SparkSchema(df.schema), "read")
    validateSchemaHasPartitionCols(df, "read")
    df
  }

  /**
   * Overwriting Hive table with different schema is allowed if table has no partitions, as table is overwritten as whole.
   */
  private def isOverwriteSchemaAllowed = (saveMode==SDLSaveMode.Overwrite || saveMode!=SDLSaveMode.OverwriteOptimized) && partitions.isEmpty

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    // validate against hive table schema if existing
    if (isTableExisting && !isOverwriteSchemaAllowed) validateSchema(SparkSchema(df.schema), SparkSchema(session.table(table.fullName).schema), "write")
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined || (connection.isDefined && connection.get.acl.isDefined), s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    require(!isRecursiveInput, "($id) HiveTableDataObject cannot write dataframe when dataobject is also used as recursive input ")
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    writeDataFrameInternal(df, createTableOnly = false, partitionValues, saveModeOptions)
  }

   /**
   * Writes DataFrame to HDFS/Parquet and creates Hive table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  private def writeDataFrameInternal(df: DataFrame, createTableOnly:Boolean, partitionValues: Seq[PartitionValues] = Seq(), saveModeOptions: Option[SaveModeOptions] = None)
                                    (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = context.sparkSession
    val dfPrepared = if (createTableOnly) session.createDataFrame(List[Row]().asJava, df.schema) else df

    // apply special save modes
    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    finalSaveMode match {
      case SDLSaveMode.Overwrite =>
        if (partitionValues.nonEmpty) { // delete concerned partitions if existing, as Spark dynamic partitioning doesn't delete empty partitions
          deletePartitionsIfExisting(partitionValues)
        }
      case SDLSaveMode.OverwriteOptimized =>
        if (partitionValues.nonEmpty) { // delete concerned partitions if existing, as append mode is used later
          deletePartitionsIfExisting(partitionValues)
        } else if (partitions.isEmpty || context.globalConfig.allowOverwriteAllPartitionsWithoutPartitionValues.contains(id)) { // delete table if existing, as append mode is used later
          dropTable
        } else {
          throw new ProcessingLogicException(s"($id) OverwriteOptimized without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
        }
      case _ => Unit
    }

    // write table and collect Spark metrics
    val metrics = SparkStageMetricsListener.execWithMetrics(this.id,
      HiveUtil.writeDfToHive(dfPrepared, hadoopPath, table, partitions, SparkSaveMode.from(finalSaveMode), numInitialHdfsPartitions = numInitialHdfsPartitions)
    )

    // apply acls
    val aclToApply = acl.orElse(connection.flatMap(_.acl))
    if (aclToApply.isDefined) AclUtil.addACLs(aclToApply.get, hadoopPath)(filesystem)

    // analyse
    if (analyzeTableAfterWrite && !createTableOnly) {
      logger.info(s"Analyze table ${table.fullName}.")
      val simpleColumns = SparkSchema(dfPrepared.schema).filter(_.dataType.isSimpleType).columns
      HiveUtil.analyze(table, simpleColumns, partitions, partitionValues)
    }

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)

    // return
    metrics
  }

  override def writeSparkDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): MetricsMap = {
    SparkStageMetricsListener.execWithMetrics( this.id,
      df.write
        .partitionBy(partitions: _*)
        .format(OutputType.Parquet.toString)
        .mode(SparkSaveMode.from(finalSaveMode))
        .save(path.toString)
    )
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
     context.sparkSession.catalog.databaseExists(table.db.get)
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    context.sparkSession.catalog.tableExists(table.db.get, table.name)
  }

  /**
   * list hive table partitions
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    implicit val session: SparkSession = context.sparkSession
    if(isTableExisting) HiveUtil.listPartitions(table, partitions)
    else Seq()
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    partitionValues.foreach(pv => HiveUtil.dropPartition(table, hadoopPath, pv, filesystem))
  }

  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    partitionValues.foreach {
      case (pvExisting, pvNew) => HiveUtil.movePartition(table, hadoopPath, pvExisting, pvNew, fileName, filesystem)
    }
    session.catalog.refreshTable(table.fullName)
  }

  override def compactPartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    CompactionUtil.compactHadoopStandardPartitions(this, partitionValues)
    session.catalog.refreshTable(table.fullName)
  }


  /**
   * Checks if partition exists and deletes it.
   * Note that partition values to check don't need to have a key/value defined for every partition column.
   */
  def deletePartitionsIfExisting(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit  = {
    val partitionValueKeys = PartitionValues.getPartitionValuesKeys(partitionValues).toSeq
    val partitionValuesToDelete = partitionValues.intersect(listPartitions.map(_.filterKeys(partitionValueKeys)))
    deletePartitions(partitionValuesToDelete)
  }

  override def createEmptyPartition(partitionValues: PartitionValues)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    if (partitionValues.keys == partitions.toSet) HiveUtil.createEmptyPartition(table, partitionValues)
    else logger.warn(s"($id) No empty partition was created for $partitionValues because there are not all partition columns defined")
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    HiveUtil.dropTable(table, hadoopPath, filesystem = Some(filesystem))
  }

  override def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String, Any] = {
    try {
      val pathStats = HdfsUtil.getPathStats(hadoopPath)(filesystem)
      val lastModifiedAt = pathStats.get(TableStatsType.LastModifiedAt.toString).map(_.asInstanceOf[Long])
      var catalogStats = HiveUtil.getCatalogStats(table)(context.sparkSession)
      val lastAnalyzedAt = catalogStats.get(TableStatsType.LastAnalyzedAt.toString).map(_.asInstanceOf[Long])
      // analyze only if table is modified after it was last analyzed
      if (update && lastModifiedAt.isDefined && (lastAnalyzedAt.isEmpty || lastAnalyzedAt.exists(lastModifiedAt.get > _))) {
        logger.info(s"($id) compute statistics: update=$update lastModifiedAt=$lastModifiedAt lastAnalyzedAt=$lastAnalyzedAt")
        try {
          HiveUtil.analyzeTable(table)(context.sparkSession)
        } catch {
          case ex: Exception => logger.warn(s"($id) failed to compute statistics ${ex.getClass.getSimpleName}: ${ex.getMessage}")
        }
      }
      val columnStats = getColumnStats(update, lastModifiedAt)
      // get catalog stats again, as they might have changed through analyzeTable or getColumnStats
      catalogStats = HiveUtil.getCatalogStats(table)(context.sparkSession)
      pathStats ++ getPartitionStats ++ catalogStats + (TableStatsType.Columns.toString -> columnStats)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get table stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map(TableStatsType.Info.toString -> e.getMessage)
    }
  }

  override def getColumnStats(update: Boolean = false, lastModifiedAt: Option[Long] = None)(implicit context: ActionPipelineContext): Map[String, Map[String, Any]] = {
    try {
      val catalogStats = HiveUtil.getCatalogStats(table)(context.sparkSession)
      val lastAnalyzedColumnsAt = catalogStats.get(TableStatsType.LastAnalyzedColumnsAt.toString).map(_.asInstanceOf[Long])
      // analyze only if table is modified after it was last analyzed
      if (update && lastModifiedAt.isDefined && (lastAnalyzedColumnsAt.isEmpty || lastAnalyzedColumnsAt.exists(lastModifiedAt.get > _))) {
        val sizeInBytes = catalogStats(TableStatsType.TableSizeInBytes.toString).asInstanceOf[BigInt]
        val metadata = context.sparkSession.sessionState.catalog.getTableMetadata(table.tableIdentifier)
        val simpleColumns = SparkSchema(metadata.schema).filter(_.dataType.isSimpleType).columns
        // analyze only if table is not too large (analyzing all columns is expensive)
        if (sizeInBytes <= Environment.analyzeTableColumnMaxBytesThreshold) {
          logger.info(s"($id) compute column statistics: update=$update lastModifiedAt=$lastModifiedAt lastAnalyzedAt=$lastAnalyzedColumnsAt sizeInBytes=$sizeInBytes")
          HiveUtil.analyzeTableColumns(table, simpleColumns)(context.sparkSession)
        } else {
          logger.warn(s"($id) Columns stats not calculated because table size ($sizeInBytes Bytes) is bigger than setting analyzeTableColumnMaxBytesThreshold (${Environment.analyzeTableColumnMaxBytesThreshold} Bytes)")
        }
      }
      // get columns stats from table
      HiveUtil.getCatalogColumnStats(table)(context.sparkSession)
    } catch {
      case e: Exception =>
        logger.error(s"($id} Could not get column stats: ${e.getClass.getSimpleName} ${e.getMessage}")
        Map()
    }
  }

  override def factory: FromConfigFactory[DataObject] = HiveTableDataObject
}

object HiveTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HiveTableDataObject = {
    extract[HiveTableDataObject](config)
  }
}
