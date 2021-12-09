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
import io.smartdatalake.definitions.{DateColumnType, Environment, OutputType, SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.{AclDef, AclUtil, CompactionUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.connection.HiveTableConnection
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

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
 * @param saveMode spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
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
                               override val schemaMin: Option[StructType] = None,
                               override var table: Table,
                               numInitialHdfsPartitions: Int = 16,
                               saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                               acl: Option[AclDef] = None,
                               connectionId: Option[ConnectionId] = None,
                               override val expectedPartitionsCondition: Option[String] = None,
                               override val housekeepingMode: Option[HousekeepingMode] = None,
                               override val metadata: Option[DataObjectMetadata] = None)
                              (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject with CanWriteDataFrame with CanHandlePartitions with HasHadoopStandardFilestore with SmartDataLakeLogger {

  // Hive tables are always written in parquet format
  private val fileName = "*.parquet"

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[HiveTableConnection](c))

  // prepare table
  table = table.overrideDb(connection.map(_.db))
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
        else HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
      }

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if(thisIsTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HiveUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HiveUtil.normalizePath(path.get)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"($id) Table ${table.fullName} exists already with different path. The table will be written with new path definition ${hadoopPathHolder}!")
      }
    }
    hadoopPathHolder
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    require(isDbExisting, s"($id) Hive DB ${table.db.get} doesn't exist (needs to be created manually).")
    if (!isTableExisting)
      require(path.isDefined, s"($id) If Hive table does not exist yet, the path must be set.")
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    val df = context.sparkSession.table(s"${table.fullName}")
    validateSchemaMin(df, "read")
    validateSchemaHasPartitionCols(df, "read")
    df
  }

  /**
   * Overwriting Hive table with different schema is allowed if table has no partitions, as table is overwritten as whole.
   */
  private def isOverwriteSchemaAllowed = (saveMode==SDLSaveMode.Overwrite || saveMode!=SDLSaveMode.OverwriteOptimized) && partitions.isEmpty

  override def init(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.init(df, partitionValues)
    validateSchemaMin(df, "write")
    validateSchemaHasPartitionCols(df, "write")
    // validate against hive table schema if existing
    if (isTableExisting && !isOverwriteSchemaAllowed) validateSchema(df, session.table(table.fullName).schema, "write")
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined || (connection.isDefined && connection.get.acl.isDefined), s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): Unit = {
    require(!isRecursiveInput, "($id) HiveTableDataObject cannot write dataframe when dataobject is also used as recursive input ")
    validateSchemaMin(df, "write")
    validateSchemaHasPartitionCols(df, "write")
    writeDataFrameInternal(df, createTableOnly = false, partitionValues, saveModeOptions)
  }

   /**
   * Writes DataFrame to HDFS/Parquet and creates Hive table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  private def writeDataFrameInternal(df: DataFrame, createTableOnly:Boolean, partitionValues: Seq[PartitionValues] = Seq(), saveModeOptions: Option[SaveModeOptions] = None)
                                    (implicit context: ActionPipelineContext): Unit = {
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
        } else if (partitions.isEmpty || Environment.globalConfig.allowOverwriteAllPartitionsWithoutPartitionValues.contains(id)) { // delete table if existing, as append mode is used later
          dropTable
        } else {
          throw new ProcessingLogicException(s"($id) OverwriteOptimized without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
        }
      case _ => Unit
    }

    // write table and fix acls
    HiveUtil.writeDfToHive( dfPrepared, hadoopPath, table, partitions, finalSaveMode.asSparkSaveMode, numInitialHdfsPartitions=numInitialHdfsPartitions )
    val aclToApply = acl.orElse(connection.flatMap(_.acl))
    if (aclToApply.isDefined) AclUtil.addACLs(aclToApply.get, hadoopPath)(filesystem)
    if (analyzeTableAfterWrite && !createTableOnly) {
      logger.info(s"Analyze table ${table.fullName}.")
      HiveUtil.analyze(table, partitions, partitionValues)
    }

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)
  }

  override def writeDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): Unit = {
    df.write
      .partitionBy(partitions:_*)
      .format(OutputType.Parquet.toString)
      .mode(finalSaveMode.asSparkSaveMode)
      .save(path.toString)
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

  override def factory: FromConfigFactory[DataObject] = HiveTableDataObject
}

object HiveTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): HiveTableDataObject = {
    extract[HiveTableDataObject](config)
  }
}
