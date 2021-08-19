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
import io.smartdatalake.definitions.{Environment, SDLSaveMode}
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionLayout, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc.DataFrameUtil.arrayToSeq
import io.smartdatalake.util.misc.{AclDef, AclUtil, DataFrameUtil, PerformanceUtils}
import io.smartdatalake.workflow.connection.HiveTableConnection
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * [[DataObject]] of type DeltaLakeTableDataObject.
 * Provides details to access Hive tables to an Action
 *
 * @param id unique name of this data object
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 * @param partitions partition columns for this data object
 * @param options Options for the Delta Lake tables (details see: https://docs.delta.io/latest/delta-batch.html)
 *                - mergeSchema: enable schema evolution
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 * @param table DeltaLake table to be written by this output
 * @param saveMode [[SDLSaveMode]] to use when writing files, default is "overwrite". Only saveMode Overwrite and Append are supported for now.
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
                                    options: Option[Map[String,String]] = None,
                                    override val schemaMin: Option[StructType] = None,
                                    override var table: Table,
                                    saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                    retentionPeriod: Option[Int] = None, // hours
                                    acl: Option[AclDef] = None,
                                    connectionId: Option[ConnectionId] = None,
                                    override val expectedPartitionsCondition: Option[String] = None,
                                    override val housekeepingMode: Option[HousekeepingMode] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalSparkTableDataObject with CanHandlePartitions with HasHadoopStandardFilestore {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[HiveTableConnection](c))

  // prepare final path and table
  @transient private var hadoopPathHolder: Path = _
  def hadoopPath(implicit session: SparkSession): Path = {
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"($id) DeltaTable ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = {
        if (thisIsTableExisting) new Path(session.sql(s"DESCRIBE DETAIL ${table.fullName}").head.getAs[String]("location"))
        else {
          val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
          val filesystem = getFilesystem(prefixedPath)
          HdfsUtil.makeAbsolutePath(prefixedPath)(filesystem)
        }
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

  table = table.overrideDb(connection.map(_.db))
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")
  }

  assert(Seq(SDLSaveMode.Overwrite,SDLSaveMode.Append).contains(saveMode), s"($id) Only saveMode Overwrite and Append supported for now.")

  override def prepare(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.prepare
    require(session.conf.getOption("spark.sql.extensions").contains("io.delta.sql.DeltaSparkSessionExtension"),
      s"($id) DeltaLake spark properties are missing. Please set spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension and spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog")
    require(isDbExisting, s"($id) DB ${table.db.get} doesn't exist (needs to be created manually).")
    if (!isTableExisting) {
      require(path.isDefined, s"($id) If DeltaLake table does not exist yet, path must be set.")
      if (filesystem.exists(hadoopPath)) {
        // define a delta table, metadata can be read from files.
        require(DeltaTable.isDeltaTable(session, hadoopPath.toString), s"($id) Path $hadoopPath is not a delta table.")
        DeltaTable.create(session).tableName(table.fullName).location(hadoopPath.toString).execute()
        logger.info(s"($id) Creating delta table ${table.fullName} for existing path $hadoopPath")
      }
    }
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    val df = session.table(table.fullName)
    validateSchemaMin(df, "read")
    validateSchemaHasPartitionCols(df, "read")
    df
  }

  override def init(df: DataFrame, partitionValues: Seq[PartitionValues])(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.init(df, partitionValues)
    validateSchemaMin(df, "write")
    validateSchemaHasPartitionCols(df, "write")
  }

  override def preWrite(implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    super.preWrite
    // validate if acl's must be / are configured before writing
    if (Environment.hadoopAuthoritiesWithAclsRequired.exists( a => filesystem.getUri.toString.contains(a))) {
      require(acl.isDefined, s"($id) ACL definitions are required for writing DataObjects on hadoop authority ${filesystem.getUri} by environment setting hadoopAuthoritiesWithAclsRequired")
    }
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false)
                             (implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    validateSchemaMin(df, "write")
    validateSchemaHasPartitionCols(df, "write")
    writeDataFrame(df, createTableOnly=false, partitionValues)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates DeltaLake table.
   * DataFrames are repartitioned in order not to write too many small files
   * or only a few HDFS files that are too large.
   */
  def writeDataFrame(df: DataFrame, createTableOnly: Boolean, partitionValues: Seq[PartitionValues])
                    (implicit session: SparkSession, context: ActionPipelineContext): Unit = {
    val dfPrepared = if (createTableOnly) {
      // create empty df with existing df's schema
      DataFrameUtil.getEmptyDataFrame(df.schema)
    } else df

    val dfWriter = dfPrepared.write
      .format("delta")
      .options(options.getOrElse(Map()))
      .option("path", hadoopPath.toString)

    if (isTableExisting) {
      if (partitions.isEmpty) {
        dfWriter
          .option("overwriteSchema", "true") // allow overwriting schema when overwriting whole table
          .mode(saveMode.asSparkSaveMode)
          .saveAsTable(table.fullName)
      } else {
        // insert
        if (saveMode == SDLSaveMode.Overwrite) {
          if (partitionValues.isEmpty) throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
          dfWriter
            .option("replaceWhere", partitionValues.map(_.getSparkExpr).reduce(_ or _).expr.sql)
            .mode(saveMode.asSparkSaveMode)
            .save() // atomic replace (replaceWhere) works only with path API
        } else {
          dfWriter
            .mode(saveMode.asSparkSaveMode)
            .saveAsTable(table.fullName)
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

  def vacuum(implicit session: SparkSession): Unit = {
    retentionPeriod.foreach { period =>
      val (_, d) = PerformanceUtils.measureDuration {
        DeltaTable.forPath(session, hadoopPath.toString).vacuum(period)
      }
      logger.info(s"($id) vacuum took $d")
    }
  }

  override def isDbExisting(implicit session: SparkSession): Boolean = {
    session.catalog.databaseExists(table.db.get)
  }

  override def isTableExisting(implicit session: SparkSession): Boolean = {
    session.catalog.tableExists(table.fullName)
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

  protected val separator: Char = Path.SEPARATOR_CHAR

  /**
   * List partitions.
   * Note that we need a Spark SQL statement as there might be partition directories with no current data inside
   */
  override def listPartitions(implicit session: SparkSession): Seq[PartitionValues] = {
    val (pvs,d) = PerformanceUtils.measureDuration(
      if(isTableExisting) PartitionValues.fromDataFrame(session.table(table.fullName).select(partitions.map(col):_*).distinct)
      else Seq()
    )
    logger.debug(s"($id) listPartitions took $d")
    return pvs
  }

  /**
   * Note that we will not delete the whole partition but just the data of the partition because delta lake keeps history
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Unit = {
    val deltaTable = DeltaTable.forName(session, table.fullName)
    partitionValues.map(_.getSparkExpr).foreach(expr => deltaTable.delete(expr))
  }

  override def dropTable(implicit session: SparkSession): Unit = HiveUtil.dropTable(table, hadoopPath, doPurge = false)

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = DeltaLakeTableDataObject

}

object DeltaLakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeltaLakeTableDataObject = {
    extract[DeltaLakeTableDataObject](config)
  }
}

