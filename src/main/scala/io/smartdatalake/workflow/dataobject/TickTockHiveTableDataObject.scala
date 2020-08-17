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
import io.smartdatalake.definitions.{DateColumnType, Environment}
import io.smartdatalake.definitions.DateColumnType.DateColumnType
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.{AclDef, AclUtil, DataFrameUtil}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.HiveTableConnection
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._

//FIXME: there's significant code duplication from HiveTableDataObject here.
case class TickTockHiveTableDataObject(override val id: DataObjectId,
                                       path: Option[String] = None,
                                       override val partitions: Seq[String] = Seq(),
                                       analyzeTableAfterWrite: Boolean = false,
                                       dateColumnType: DateColumnType = DateColumnType.Date,
                                       override val schemaMin: Option[StructType] = None,
                                       override var table: Table,
                                       numInitialHdfsPartitions: Int = 16,
                                       saveMode: SaveMode = SaveMode.Overwrite,
                                       acl: Option[AclDef] = None,
                                       connectionId: Option[ConnectionId] = None,
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

  // prepare final path
  @transient private var hadoopPathHolder: Path = _
  def hadoopPath(implicit session: SparkSession): Path = {
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"TickTockHiveTable ${table.fullName} does not exist, so path must be set.")

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
          logger.warn(s"Table ${table.fullName} exists already with different path. The table will be written with new path definition ${hadoopPathHolder}!")
      }
    }
    hadoopPathHolder
  }

  @transient private var filesystemHolder: FileSystem = _
  def filesystem(implicit session: SparkSession): FileSystem = {
    if (filesystemHolder == null) {
      filesystemHolder = HdfsUtil.getHadoopFsFromSpark(hadoopPath)
    }
    filesystemHolder
  }

  override def prepare(implicit session: SparkSession): Unit = {
    require(isDbExisting, s"($id) Hive DB ${table.db.get} doesn't exist (needs to be created manually).")
    if (!isTableExisting)
      require(path.isDefined, "If Hive table does not exist yet, the path must be set.")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {
    val df = if(!isTableExisting && schemaMin.isDefined) {
      logger.info(s"Table ${table.fullName} does not exist but schemaMin was provided. Creating empty DataFrame.")
      DataFrameUtil.getEmptyDataFrame(schemaMin.get)
    }
    else {
      session.table(s"${table.fullName}")
    }

    validateSchemaMin(df)
    df
  }

  override def preWrite(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined || (connection.isDefined && connection.get.acl.isDefined), s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues])
                             (implicit session: SparkSession): Unit = {
    validateSchemaMin(df)
    writeDataFrame(df, createTableOnly=false, partitionValues)

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates Hive table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  def writeDataFrame(df: DataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues])
                    (implicit session: SparkSession): Unit = {
    val dfPrepared = if (createTableOnly) {
      // create empty df with existing df's schema
      session.createDataFrame(List.empty[Row].asJava, df.schema)
    } else {
      // pass DataFrame straight through if numInitialHdfsPartitions == -1, in this case the file size in the responsibility of the framework and must be controlled in custom transformations
      if(numInitialHdfsPartitions == -1) df
      // estimate number of partitions from existing data, otherwise use numInitialHdfsPartitions
      else if (isTableExisting) {
        val currentHdfsPath = HdfsUtil.prefixHadoopPath(HiveUtil.existingTickTockLocation(table.db.get, session, table.name), None)
        HdfsUtil.repartitionForHdfsFileSize(df, currentHdfsPath.toString)
      } else df.repartition(numInitialHdfsPartitions)
    }

    // write table and fix acls
    HiveUtil.writeDfToHiveWithTickTock(session, dfPrepared, hadoopPath.toString, table.name, table.db.get, partitions, saveMode)
    val aclToApply = acl.orElse(connection.flatMap(_.acl))
    if (aclToApply.isDefined) AclUtil.addACLs(aclToApply.get, hadoopPath)(filesystem)
    if (analyzeTableAfterWrite && !createTableOnly) {
      logger.info(s"($id) Analyze table ${table.fullName}.")
      HiveUtil.analyze(session, table.db.get, table.name, partitions, partitionValues)
    }
  }

  override def isDbExisting(implicit session: SparkSession): Boolean = {
    session.catalog.databaseExists(table.db.get)
  }

  override def isTableExisting(implicit session: SparkSession): Boolean = {
    session.catalog.tableExists(table.db.get, table.name)
  }

  /**
   * list hive table partitions
   */
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    if(isTableExisting) HiveUtil.listPartitions(table, partitions)
    else Seq()
  }

  override def createEmptyPartition(partitionValues: PartitionValues)(implicit session: SparkSession): Unit = {
    if (partitionValues.keys == partitions.toSet) HiveUtil.createEmptyPartition(table, partitionValues)
    else logger.warn(s"($id) No empty partition was created for $partitionValues because there are not all partition columns defined")
  }

  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    partitionValues.foreach( pv => HiveUtil.dropPartition(table, hadoopPath, pv))
  }

  override def dropTable(implicit session: SparkSession): Unit = {
    HiveUtil.dropTable(session, table.db.get, table.name)
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = TickTockHiveTableDataObject
}


object TickTockHiveTableDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): TickTockHiveTableDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[TickTockHiveTableDataObject].value
  }
}
