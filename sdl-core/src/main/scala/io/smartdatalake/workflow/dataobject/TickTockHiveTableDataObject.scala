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
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.definitions.DateColumnType.DateColumnType
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{DateColumnType, Environment, SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.{AclDef, AclUtil, EnvironmentUtil}
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.HiveTableConnection
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe.typeOf

//FIXME: there's significant code duplication from HiveTableDataObject here.
case class TickTockHiveTableDataObject(override val id: DataObjectId,
                                       path: Option[String] = None,
                                       override val partitions: Seq[String] = Seq(),
                                       analyzeTableAfterWrite: Boolean = false,
                                       dateColumnType: DateColumnType = DateColumnType.Date,
                                       override val schemaMin: Option[GenericSchema] = None,
                                       override var table: Table,
                                       numInitialHdfsPartitions: Int = 16,
                                       saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                       acl: Option[AclDef] = None,
                                       connectionId: Option[ConnectionId] = None,
                                       override val expectedPartitionsCondition: Option[String] = None,
                                       override val housekeepingMode: Option[HousekeepingMode] = None,
                                       override val metadata: Option[DataObjectMetadata] = None)
                                      (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with CanHandlePartitions {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[HiveTableConnection](c))

  // prepare table
  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  assert(saveMode!=SDLSaveMode.OverwritePreserveDirectories, s"($id) saveMode OverwritePreserveDirectories not supported for now.")
  assert(saveMode!=SDLSaveMode.OverwriteOptimized, s"($id) saveMode OverwriteOptimized not supported for now.")

  // prepare final path
  @transient private var hadoopPathHolder: Path = _
  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    implicit val session: SparkSession = context.sparkSession
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"TickTockHiveTable ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = {
        if (thisIsTableExisting) HiveUtil.removeTickTockFromLocation(new Path(HiveUtil.existingTableLocation(table)))
        else HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
      }

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if(thisIsTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HiveUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HiveUtil.normalizePath(path.get)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"Table ${table.fullName} exists already with different path $path. The table will be written with new path definition $hadoopPathHolder!")
      }
    }
    hadoopPathHolder
  }

  @transient private var filesystemHolder: FileSystem = _
  def filesystem(implicit context: ActionPipelineContext): FileSystem = {
    implicit val session: SparkSession = context.sparkSession
    if (filesystemHolder == null) {
      filesystemHolder = HdfsUtil.getHadoopFsFromSpark(hadoopPath)
    }
    filesystemHolder
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    require(isDbExisting, s"($id) Hive DB ${table.db.get} doesn't exist (needs to be created manually).")
    if (!isTableExisting)
      require(path.isDefined, "If Hive table does not exist yet, the path must be set.")
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.initSparkDataFrame(df, partitionValues)
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
  }

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    val df = if(!isTableExisting && schemaMin.isDefined) {
      logger.info(s"Table ${table.fullName} does not exist but schemaMin was provided. Creating empty DataFrame.")
      DataFrameUtil.getEmptyDataFrame(schemaMin.map(_.convert(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema].inner).get)
    } else {
      session.table(s"${table.fullName}")
    }

    validateSchemaMin(SparkSchema(df.schema), "read")
    validateSchemaHasPartitionCols(df, "read")
    df
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined || (connection.isDefined && connection.get.acl.isDefined), s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): Unit = {
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    writeDataFrameInternal(df, createTableOnly=false, partitionValues, isRecursiveInput, saveModeOptions)

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates Hive table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  def writeDataFrameInternal(df: DataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues], isRecursiveInput: Boolean, saveModeOptions: Option[SaveModeOptions])
                            (implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    val dfPrepared = if (createTableOnly) {
      // create empty df with existing df's schema
      DataFrameUtil.getEmptyDataFrame(df.schema)
    } else {
      if (numInitialHdfsPartitions == -1) {
        // pass DataFrame straight through if numInitialHdfsPartitions == -1, in this case the file size in the responsibility of the framework and must be controlled in custom transformations
        df
      } else if (EnvironmentUtil.isSparkAdaptiveQueryExecEnabled) {
        // pass DataFrame straight through if AQE is enabled
        logger.warn(s"($id) numInitialHdfsPartitions is ignored when Spark 3.0 Adaptive Query Execution (AQE) is enabled")
        df
      } else if (isTableExisting) {
        // estimate number of partitions from existing data, otherwise use numInitialHdfsPartitions
        val currentHdfsPath = HdfsUtil.prefixHadoopPath(HiveUtil.existingTickTockLocation(table), None)
        HdfsUtil.repartitionForHdfsFileSize(df, currentHdfsPath)
      } else df.repartition(numInitialHdfsPartitions)
    }

    // write table and fix acls
    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    HiveUtil.writeDfToHiveWithTickTock(dfPrepared, hadoopPath, table, partitions, SparkSaveMode.from(finalSaveMode), forceTickTock = isRecursiveInput)
    val aclToApply = acl.orElse(connection.flatMap(_.acl))
    if (aclToApply.isDefined) AclUtil.addACLs(aclToApply.get, hadoopPath)(filesystem)
    if (analyzeTableAfterWrite && !createTableOnly) {
      logger.info(s"($id) Analyze table ${table.fullName}.")
      HiveUtil.analyze(table, partitions, partitionValues)
    }
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

  override def createEmptyPartition(partitionValues: PartitionValues)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    if (partitionValues.keys == partitions.toSet) HiveUtil.createEmptyPartition(table, partitionValues)
    else logger.warn(s"($id) No empty partition was created for $partitionValues because there are not all partition columns defined")
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    partitionValues.foreach( pv => HiveUtil.dropPartition(table, hadoopPath, pv, filesystem))
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    HiveUtil.dropTable(table, hadoopPath, filesystem = Some(filesystem))
  }

  override def factory: FromConfigFactory[DataObject] = TickTockHiveTableDataObject
}


object TickTockHiveTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): TickTockHiveTableDataObject = {
    extract[TickTockHiveTableDataObject](config)
  }
}
