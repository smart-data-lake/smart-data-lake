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

import io.delta.tables.DeltaTable
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.definitions.{DateColumnType, Environment}
import io.smartdatalake.util.misc.DataFrameUtil.arrayToSeq
import io.smartdatalake.definitions.DateColumnType.DateColumnType
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.misc.{AclDef, AclUtil, PerformanceUtils}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.connection.HiveTableConnection
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
 * [[DataObject]] of type DeltaLakeTableDataObject.
 * Provides details to access Hive tables to an Action
 *
 * @param id unique name of this data object
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 * @param partitions partition columns for this data object
 * @param dateColumnType type of date column
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param table DeltaLake table to be written by this output
 * @param numInitialHdfsPartitions number of files created when writing into an empty table (otherwise the number will be derived from the existing data)
 * @param saveMode spark [[SaveMode]] to use when writing files, default is "overwrite"
 * @param retentionPeriod Optional delta lake retention threshold in hours. Files required by the table for reading versions earlier than this will be preserved and the rest of them will be deleted.
 * @param acl override connections permissions for files created tables hadoop directory with this connection
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param metadata meta data
 */
case class DeltaLakeTableDataObject(override val id: DataObjectId,
                                    path: String,
                                    override val partitions: Seq[String] = Seq(),
                                    dateColumnType: DateColumnType = DateColumnType.Date,
                                    override val schemaMin: Option[StructType] = None,
                                    override var table: Table,
                                    numInitialHdfsPartitions: Int = 16,
                                    saveMode: SaveMode = SaveMode.Overwrite,
                                    retentionPeriod: Option[Int] = None, // hours
                                    acl: Option[AclDef] = None,
                                    connectionId: Option[ConnectionId] = None,
                                    override val expectedPartitionsCondition: Option[String] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with CanHandlePartitions {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[HiveTableConnection](c))

  // prepare final path and table
  @transient private[workflow] lazy val hadoopPath = HdfsUtil.prefixHadoopPath(path, connection.map(_.pathPrefix))
  @transient private var filesystemHolder: FileSystem = _
  def filesystem(implicit session: SparkSession): FileSystem = {
    if (filesystemHolder == null) {
      filesystemHolder = HdfsUtil.getHadoopFsFromSpark(hadoopPath)
    }
    filesystemHolder
  }
  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")
  }

  override def prepare(implicit session: SparkSession): Unit = {
    super.prepare
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession): DataFrame = {
    val df = session.read.format("delta").load(hadoopPath.toString)
    validateSchemaMin(df)
    df
  }

  override def preWrite(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined, s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false)
                             (implicit session: SparkSession): Unit = {
    validateSchemaMin(df)
    writeDataFrame(df, createTableOnly=false, partitionValues)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates DeltaLake table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  def writeDataFrame(df: DataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues])
                    (implicit session: SparkSession): Unit = {
    val dfPrepared = if (createTableOnly) {
      // create empty df with existing df's schema
      session.createDataFrame(List.empty[Row].asJava, df.schema)
    } else df.repartition(Math.max(1,numInitialHdfsPartitions))


    // write table
    val deltaTableWriter = dfPrepared.write.format("delta")
    if (partitions.isEmpty) deltaTableWriter.mode(saveMode).save(hadoopPath.toString) else {
      deltaTableWriter.partitionBy(partitions:_*).mode(saveMode).save(hadoopPath.toString)
    }

    // vacuum delta lake table
    vacuum

    // fix acls
    if (acl.isDefined) AclUtil.addACLs(acl.get, hadoopPath)(filesystem)
  }

  def vacuum(implicit session: SparkSession): Unit = {
    retentionPeriod.foreach { period =>
      val (_, d) = PerformanceUtils.measureDuration {
        DeltaTable.forPath(session, hadoopPath.toString).vacuum(period)
      }
      logger.info(s"($id) vacuumed delta lake table: duration=$d")
    }
  }

  // Delta Lake is not connected to Hive Metastore. It is a file based Spark API.
  // We therefore cannot check isDbExisting.
  override def isDbExisting(implicit session: SparkSession): Boolean = true

  // Delta Lake is not connected to Hive Metastore. It is a file based Spark API.
  // We therefore cannot check isTableExisting.
  override def isTableExisting(implicit session: SparkSession): Boolean = true

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
  protected def checkFilesExisting(implicit session:SparkSession): Boolean = {
    val files = if (filesystem.exists(hadoopPath.getParent)) {
      arrayToSeq(filesystem.globStatus(hadoopPath))
    } else {
      Seq.empty
    }

    if (files.isEmpty) {
      logger.warn(s"($id) No files found at $hadoopPath. Can not import any data.")
      require(!failIfFilesMissing, s"($id) failIfFilesMissing is enabled and no files to process have been found in $hadoopPath.")
    }

    files.nonEmpty
  }

  protected val separator: Char = Environment.defaultPathSeparator

  /**
   * Return a [[String]] specifying the partition layout.
   *
   * For Hadoop the default partition layout is colname1=<value1>/colname2=<value2>/.../
   */
  final def partitionLayout(): Option[String] = {
    if (partitions.nonEmpty) {
      Some(HdfsUtil.getHadoopPartitionLayout(partitions, separator))
    } else {
      None
    }
  }

  /**
   * List partitions on data object's root path
   */
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    partitionLayout().map {
      partitionLayout =>
        // get search pattern for root directory
        val pattern = PartitionLayout.replaceTokens(partitionLayout, PartitionValues(Map()))
        // list directories and extract partition values
        filesystem.globStatus( new Path(hadoopPath, pattern))
          .filter{fs => fs.isDirectory}
          .map(_.getPath.toString)
          .map( path => PartitionLayout.extractPartitionValues(partitionLayout, "", path + separator))
          .toSeq
    }.getOrElse(Seq())
  }

  override def dropTable(implicit session: SparkSession): Unit = throw new NotImplementedError()

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = TickTockHiveTableDataObject

}



