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
import io.smartdatalake.util.misc.{AclDef, AclUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.connection.HiveTableConnection
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.streaming.Trigger
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
 * @param partitions partition columns for this data object
 * @param acl override connections permissions for files created tables hadoop directory with this connection
 * @param analyzeTableAfterWrite enable compute statistics after writing data (default=false)
 * @param dateColumnType type of date column
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param saveMode spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param numInitialHdfsPartitions number of files created when writing into an empty table (otherwise the number will be derived from the existing data)
 * @param metadata meta data
 */
case class HiveTableDataObject(override val id: DataObjectId,
                               path: String,
                               override val partitions: Seq[String] = Seq(),
                               analyzeTableAfterWrite: Boolean = false,
                               dateColumnType: DateColumnType = DateColumnType.Date,
                               override val schemaMin: Option[StructType] = None,
                               override var table: Table,
                               numInitialHdfsPartitions: Int = 16,
                               saveMode: SaveMode = SaveMode.Overwrite,
                               acl: Option[AclDef] = None,
                               connectionId: Option[ConnectionId] = None,
                               override val streamingOptions: Map[String, String] = Map(),
                               override val trigger: Trigger = Trigger.Once,
                               override val metadata: Option[DataObjectMetadata] = None)
                              (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TableDataObject with CanWriteDataFrame with CanWriteDataStream with CanHandlePartitions with SmartDataLakeLogger {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[HiveTableConnection](c))

  // prepare final path and table
  @transient private[workflow] lazy val hadoopPath = HdfsUtil.prefixHadoopPath(path, connection.map(_.pathPrefix))
  @transient private var filesystemHolder: FileSystem = _
  def filesystem(implicit session: SparkSession): FileSystem = {
    if (filesystemHolder==null) {
      filesystemHolder = HdfsUtil.getHadoopFsFromSpark(hadoopPath)
    }
    filesystemHolder
  }
  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection.")

  override def prepare(implicit session: SparkSession): Unit = {
    require(isDbExisting, s"($id) Hive DB ${table.db.get} doesn't exist (needs to be created manually).")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {
    val df = session.table(s"${table.fullName}")
    validateSchemaMin(df)
    df
  }

  override def preWrite(implicit session: SparkSession): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined, s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues])
                             (implicit session: SparkSession): Unit = {
    validateSchemaMin(df)
    writeDataFrame(df, createTableOnly = false, partitionValues)
  }

   /**
   * Writes DataFrame to HDFS/Parquet and creates Hive table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  private def writeDataFrame(df: DataFrame, createTableOnly:Boolean, partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): Unit = {
    val dfPrepared = if (createTableOnly) session.createDataFrame(List[Row]().asJava, df.schema) else df

    // write table and fix acls
    HiveUtil.writeDfToHive( session, dfPrepared, hadoopPath.toString, table.name, table.db.get, partitions, saveMode, numInitialHdfsPartitions=numInitialHdfsPartitions )
    if (acl.isDefined) AclUtil.addACLs(acl.get, hadoopPath)(filesystem)
    if (analyzeTableAfterWrite && !createTableOnly) {
      logger.info(s"Analyze table ${table.fullName}.")
      HiveUtil.analyze(session, table.db.get, table.name, partitions, partitionValues)
    }

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)
  }



  override def init(df: DataFrame, partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    // on write: create tables if possible
    require(isDbExisting, s"Hive DB ${table.db.get} doesn't exist (needs to be created manually).")
    if (!isTableExisting) {
      logger.info(s"Creating table ${table.fullName}.")
      writeDataFrame(df, createTableOnly = true, partitionValues)
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

  override def dropTable(implicit session: SparkSession): Unit = {
    HiveUtil.dropTable(session, table.db.get, table.name)
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = HiveTableDataObject
}

object HiveTableDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): HiveTableDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[HiveTableDataObject].value
  }
}
