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

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.connection.IcebergTableConnection
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.file.HasHadoopStandardFilestore
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.hadoop.fs.Path

import scala.reflect.runtime.universe.Type

/**
 * [[DataObject]] of type IcebergTableDataObject.
 * Provides details to access Tables in Iceberg format to an Action.
 *
 * Iceberg format maintains a transaction log in a separate metadata subfolder.
 * The schema is registered in Metastore by IcebergTableDataObject.
 * For this either the default spark catalog must be wrapped in an IcebergSessionCatalog,
 * or an additional IcebergCatalog has to be configured. See also [[https://iceberg.apache.org/docs/latest/getting-started/]].
 *
 * The following anomalies between metastore and filesystem might occur:
 * - table is registered in metastore but path does not exist -> table is dropped from metastore
 * - table is registered in metastore but path is empty -> error is thrown. Delete the path manually to clean up.
 * - table is registered and path contains parquet files, but metadata subfolder is missing -> path is converted to Iceberg format
 * - table is not registered but path contains parquet files and metadata subfolder -> Table is registered in catalog
 * - table is not registered but path contains parquet files without metadata subfolder -> path is converted to Iceberg format and table is registered in catalog
 * - table is not registered and path does not exist -> table is created on write
 *
 * IcebergTableDataObject implements
 * - [[CanMergeDataFrame]] by writing a temp table and using one SQL merge statement.
 * - [[CanEvolveSchema]] by using internal Iceberg API.
 * - Overwriting partitions is implemented by using DataFrameWriterV2.overwrite(condition) API in one transaction.
 *
 * Pick this DataObject over a plain file DataObject if you need ACID transactions, merge (upsert), schema evolution
 * or snapshot expiration on a table, and over DeltaLakeTableDataObject if your platform standardizes on Iceberg.
 * Note that `partitions` are always mapped to Iceberg identity transforms; other Iceberg partition transforms
 * (bucket, truncate, days, ...) cannot be configured from SDLB.
 *
 * Example:
 * {{{
 * dataObjects = {
 *   int-airports {
 *     type = IcebergTableDataObject
 *     path = "~{env.basedir}/int_airports"
 *     table = { db = "default", name = "int_airports", primaryKey = [ident] }
 *     partitions = [country]
 *     saveMode = Merge
 *     allowSchemaEvolution = true
 *   }
 * }
 * }}}
 *
 * @param path                   hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *                               If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 *                               If Iceberg table is defined on a hadoop catalog, path must be None as it is defined through the catalog directory structure.
 * @param options                Options for Iceberg tables see: [[https://iceberg.apache.org/docs/latest/configuration/]]
 * @param table                  Iceberg table to be written by this output
 * @param saveMode               [[SDLSaveMode]] to use when writing files, default is "Overwrite". Overwrite, Append and Merge are supported for now.
 * @param allowSchemaEvolution   If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param historyRetentionPeriod Optional Iceberg retention threshold in hours. Files required by the table for reading versions younger than retentionPeriod will be preserved and the rest of them will be deleted.
 * @param connectionId           optional id of [[IcebergTableConnection]]
 * @param metadata               metadata
 * @param preReadSql             SQL-statement to be executed in exec phase before reading input table. If the catalog and/or schema are not
 *                               explicitly defined, the ones present in the configured "table" object are used.
 * @param postReadSql            SQL-statement to be executed in exec phase after reading input table and before action is finished. If the catalog and/or schema are not
 *                               explicitly defined, the ones present in the configured "table" object are used.
 * @param preWriteSql            SQL-statement to be executed in exec phase before writing output table. If the catalog and/or schema are not
 *                               explicitly defined, the ones present in the configured "table" object are used.
 * @param postWriteSql           SQL-statement to be executed in exec phase after writing output table. If the catalog and/or schema are not
 *                               explicitly defined, the ones present in the configured "table" object are used.
 *
 * @note If the Iceberg table is defined on a hadoop catalog, `path` must not be set as it is derived from the catalog
 *       directory structure.
 * @note The engine specific implementation is provided by module sdl-iceberg (classic Spark) or sdl-sparkconnect
 *       (Spark Connect), see [[IcebergTableEngine]].
 * @see [[IcebergTableConnection]] to share catalog, db and path prefix between multiple Iceberg DataObjects.
 */
case class IcebergTableDataObject(override val id: DataObjectId,
                                  path: Option[String] = None,
                                  override val partitions: Seq[String] = Seq(),
                                  options: Map[String, String] = Map(),
                                  override val schemaMin: Option[GenericSchema] = None,
                                  override var table: Table,
                                  override val constraints: Seq[Constraint] = Seq(),
                                  override val expectations: Seq[Expectation] = Seq(),
                                  saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                  override val allowSchemaEvolution: Boolean = false,
                                  historyRetentionPeriod: Option[Int] = None, // hours
                                  connectionId: Option[ConnectionId] = None,
                                  override val expectedPartitionsCondition: Option[String] = None,
                                  override val housekeepingMode: Option[HousekeepingMode] = None,
                                  override val metadata: Option[DataObjectMetadata] = None,
                                  override val preReadSql: Option[String] = None,
                                  override val postReadSql: Option[String] = None,
                                  override val preWriteSql: Option[String] = None,
                                  override val postWriteSql: Option[String] = None,
                                 )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanMergeDataFrame with CanEvolveSchema with CanHandlePartitions
    with HasHadoopStandardFilestore with ExpectationValidation with CanCreateIncrementalOutput
    with CanHandleCatalogMetadata
    with HasEngineImplementation[IcebergTableEngine] {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  val connection: Option[IcebergTableConnection] = connectionId.map(c => getConnection[IcebergTableConnection](c))

  // prepare final path and table
  @transient private var hadoopPathHolder: Path = _

  private[smartdatalake] val filetypePattern: String = ".*(\\.parquet|\\.avro|\\.orc|c\\d\\d\\d)$" // Iceberg supports to read mixed tables! 'c000' can be the file ending for parquet files of legacy hive tables!

  override protected def createEngines: Seq[IcebergTableEngine] =
    DataObjectEngine.createEngines[IcebergTableEngine, IcebergTableDataObject](this, classOf[IcebergTableDataObject])

  override protected def engineNotFoundHint: String =
    "Add module sdl-iceberg (classic Spark engine) or sdl-sparkconnect (Spark Connect engine) to the classpath."

  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    // location as defined by the catalog: default path of a path based catalog, or the location of an existing table
    val catalogLocation = engine.getTableLocation
    require(catalogLocation.isDefined || path.isDefined, s"($id) Iceberg table ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = catalogLocation.map(new Path(_)).getOrElse(getAbsolutePath)

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if (isTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HdfsUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HdfsUtil.normalizePath(getAbsolutePath.toString)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"($id) Table ${table.fullName} exists already with different path $hadoopPathHolder. New path definition $getAbsolutePath is ignored!")
      }
    }
    hadoopPathHolder
  }

  private def getAbsolutePath(implicit context: ActionPipelineContext) = {
    val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
    HdfsUtil.makeAbsolutePath(prefixedPath)(getFilesystem(prefixedPath, context.serializableHadoopConf)) // don't use "filesystem" to avoid loop
  }

  table = table.overrideCatalogAndDb(connection.flatMap(_.catalog), connection.map(_.db))
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  // prepare tmp table used for merge statement
  private[smartdatalake] val tmpTable: Table = {
    val tmpTableName = s"${table.name}_sdltmp"
    table.copy(name = tmpTableName)
  }

  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.Merge).contains(saveMode), s"($id) Only saveMode Overwrite, Append and Merge supported for now.")

  private[smartdatalake] def getMetadataPath(implicit context: ActionPipelineContext): Path = {
    options.get("write.metadata.path").map(new Path(_))
      .getOrElse(new Path(hadoopPath,"metadata"))
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    if (!isDbExisting) {
      // DB (schema) is created automatically by iceberg when creating tables. But we would like to keep the same behaviour as done by spark_catalog, where only default DB is existing, and others must be created manually.
      require(table.db.contains("default"), s"($id) DB ${table.db.get} doesn't exist (needs to be created manually).")
    }
    // engine-specific preparation, e.g. checking spark options and registering/converting an existing table path
    engine.prepare()
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
    if (isTableExisting)
      validateSchemaHasPrimaryKeyCols(getDataFrame().schema.columns, role = "prepare", obj = "Existing table")
  }

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = getSubFeedSupportedTypes.head)(implicit context: ActionPipelineContext): GenericDataFrame = {
    val df = engine(subFeedType).getDataFrame(partitionValues, incrementalOutputExpr)
    validateSchemaMin(df.schema, "read")
    validateSchemaHasPartitionCols(df.schema.columns, "read")
    df
  }

  override def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    DataFrameSubFeed.getCompanion(subFeedType).getSubFeed(getDataFrame(partitionValues, subFeedType), id, partitionValues)
  }

  override def init(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    val targetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df)
    validateSchemaMin(targetDf.schema, "write")
    validateSchemaHasPartitionCols(targetDf.schema.columns, "write")
    validateSchemaHasPrimaryKeyCols(targetDf.schema.columns, "write")
    if (isTableExisting && !allowSchemaEvolution) {
      validateSchema(targetDf.schema, getDataFrame(Seq(), df.subFeedType).schema, "write")
    }
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates Iceberg table.
   */
  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    val writeEngine = engine(df.subFeedType)
    val metrics = writeEngine.writeDataFrame(df, partitionValues, saveModeOptions)
    // vacuum iceberg table
    writeEngine.vacuum()
    metrics
  }

  def vacuum(implicit context: ActionPipelineContext): Unit = engine.vacuum()

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = engine.isDbExisting

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = engine.isTableExisting

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
  private[smartdatalake] def checkFilesExisting(implicit context: ActionPipelineContext): Boolean = {
    val hasFiles = filesystem.exists(hadoopPath.getParent) &&
      HdfsUtil.listFiles(hadoopPath, recursive = true, filterFun = s => s.isDirectory || s.getPath.getName.matches(filetypePattern))(filesystem).nonEmpty
    if (!hasFiles) {
      logger.warn(s"($id) No files found at $hadoopPath. Can not import any data.")
      require(!failIfFilesMissing, s"($id) failIfFilesMissing is enabled and no files to process have been found in $hadoopPath.")
    }
    hasFiles
  }

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = engine.listPartitions

  /**
   * Note that Iceberg will not delete the whole partition but just the data of the partition because Iceberg keeps history
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = engine.deletePartitions(partitionValues)

  override def dropTable(implicit context: ActionPipelineContext): Unit = engine.dropTable

  override def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String, Any] = engine.getStats(update)

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])
                             (implicit context: ActionPipelineContext): Map[String, Map[String, Any]] = engine.getColumnStats(update, lastModifiedAt)

  private[smartdatalake] var incrementalOutputExpr: Option[String] = None

  /**
   * To implement incremental processing this function is called to initialize the DataObject with its state from the last increment.
   * The state is just a string. Its semantics is internal to the DataObject.
   * Note that this method is called on initialization of the SmartDataLakeBuilder job (init Phase)
   * and for streaming execution after every execution of an Action involving this DataObject (postExec).
   *
   * @param state Internal state of last increment. If None then the first increment (maybe a full increment) is delivered.
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    incrementalOutputExpr = state.orElse(Some("0"))
  }

   /**
   * Return the state of the last increment or empty if no increment was processed.
   */
  override def getState: Option[String] = {
    incrementalOutputExpr
  }

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    sqlOpt.foreach(stmt => SQLUtil.execSqlBasedOnTable(stmt, table, engine.sql, s"($id) "))
  }

  override def getTableComment(implicit context: ActionPipelineContext): Option[String] =
    CatalogMetadataSqlUtil.getTableComment(table, engine.sql)

  override def setTableComment(comment: String)(implicit context: ActionPipelineContext): Unit =
    CatalogMetadataSqlUtil.setTableComment(table, comment, engine.sql, s"($id) ")

  override def setColumnComments(comments: Map[Seq[String], String])(implicit context: ActionPipelineContext): Unit =
    CatalogMetadataSqlUtil.setColumnComments(table, comments, engine.sql, s"($id) ")
}

object IcebergTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): IcebergTableDataObject = {
    extract[IcebergTableDataObject](config)
  }
}

/**
 * SPI for engine-specific implementations of [[IcebergTableDataObject]].
 *
 * Implementations are discovered on the classpath and must have a public constructor
 * with a single IcebergTableDataObject parameter, see [[DataObjectEngine]].
 * The classic Spark implementation is provided by module sdl-iceberg,
 * the Spark Connect implementation by module sdl-sparkconnect.
 */
private[smartdatalake] trait IcebergTableEngine extends DataObjectEngine {

  /**
   * Read the table, incl. incremental CDC read when incrementalOutputExpr is set (DataObjectStateIncrementalMode).
   */
  def getDataFrame(partitionValues: Seq[PartitionValues], incrementalOutputExpr: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame

  /**
   * Write/merge the DataFrame to the table, incl. NoDataToProcessWarning check and Iceberg snapshot metrics.
   */
  def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap

  /**
   * Engine-specific preparation, e.g. checking spark options and registering/converting an existing table path (classic Spark only).
   */
  def prepare()(implicit context: ActionPipelineContext): Unit

  /**
   * Expire snapshots older than historyRetentionPeriod.
   */
  def vacuum()(implicit context: ActionPipelineContext): Unit

  def isDbExisting(implicit context: ActionPipelineContext): Boolean

  def isTableExisting(implicit context: ActionPipelineContext): Boolean

  def dropTable(implicit context: ActionPipelineContext): Unit

  /**
   * Table location as defined by the catalog, e.g. the default path of a path based catalog (HadoopCatalog),
   * or the location of an existing table. If None, the DataObject falls back to the configured path.
   */
  def getTableLocation(implicit context: ActionPipelineContext): Option[String]

  def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues]

  def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit

  def getStats(update: Boolean)(implicit context: ActionPipelineContext): Map[String, Any]

  def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])(implicit context: ActionPipelineContext): Map[String, Map[String, Any]]

  /**
   * Execute a SQL statement. Basis for generic SQL composition in the DataObject (e.g. pre/post SQL).
   */
  def sql(stmt: String)(implicit context: ActionPipelineContext): GenericDataFrame
}
