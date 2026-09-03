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
import io.smartdatalake.workflow.connection.DeltaLakeTableConnection
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.file.HasHadoopStandardFilestore
import io.smartdatalake.workflow.dataobject.generic._
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.hadoop.fs.Path

import java.sql.SQLException
import scala.reflect.runtime.universe.Type
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
 * - table is not registered and path does not exist -> table is created on write
 *
 *  * DeltaLakeTableDataObject implements
 * - [[CanMergeDataFrame]] by using DeltaTable.merge API.
 * - [[CanEvolveSchema]] by using mergeSchema option.
 * - Overwriting partitions is implemented by replaceWhere option in one transaction.
 *
 * Use this DataObject instead of a plain file DataObject if you need ACID transactions, merge (upsert) or schema
 * evolution on a table.
 *
 * Example:
 * {{{
 * dataObjects = {
 *   int-airports {
 *     type = DeltaLakeTableDataObject
 *     path = "~{env.basedir}/int_airports"
 *     table = { db = "default", name = "int_airports", primaryKey = [ident] }
 *     saveMode = Merge
 *     allowSchemaEvolution = true
 *     retentionPeriod = 168
 *   }
 * }
 * }}}
 *
 * @param id unique name of this data object
 * @param path Optional hadoop directory for this table. If path is not defined, table is handled as a managed table.
 *             If it doesn't contain scheme and authority, the connections pathPrefix is applied.
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
 * @param saveMode     [[SDLSaveMode]] to use when writing files, default is "Overwrite". Overwrite, Append and Merge are supported for now.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param retentionPeriod Optional delta lake retention threshold in hours. Files required by the table for reading versions younger than retentionPeriod will be preserved and the rest of them will be deleted.
 * @param minVacuumInterval Optional String to determine the minimum time interval between two vacuum operations. If the parameter is set,
 *                          SDLB will look at the last vacuum-execution time in the table and compare it to the current time. If the parameter is not set or if a vacuum has never happened, it will vacuum the table.
 *                          The interval must be provided as a String in ISO 8601 Duration format (e.g. "P4DT12H" for "four days and twelve hours")
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write.
 *                         E.g. it can be used to clean up, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param metadata metadata of the table. metadata.description is applied as table comment in the catalog
 *                 by the DataObjectSchemaExporter, see also [[io.smartdatalake.workflow.dataobject.generic.CanHandleCatalogMetadata]].
 *
 * @note DeltaLake needs the spark properties spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension and
 *       spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog. They are added automatically
 *       by DeltaLakeModulePlugin when SDLB creates the SparkSession (except on Databricks, where they are preset),
 *       so normally no manual configuration is needed.
 * @see [[io.smartdatalake.workflow.connection.DeltaLakeTableConnection]] to share catalog, db and path prefix between
 *      multiple DeltaLake DataObjects.
 */
case class DeltaLakeTableDataObject(override val id: DataObjectId,
                                    path: Option[String] = None,
                                    override val partitions: Seq[String] = Seq(),
                                    options: Map[String,String] = Map(),
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
                                    minVacuumInterval: Option[String] = None,
                                    connectionId: Option[ConnectionId] = None,
                                    override val expectedPartitionsCondition: Option[String] = None,
                                    override val housekeepingMode: Option[HousekeepingMode] = None,
                                    override val metadata: Option[DataObjectMetadata] = None)
                                   (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanMergeDataFrame with CanEvolveSchema with CanHandlePartitions
    with HasHadoopStandardFilestore with ExpectationValidation with CanCreateIncrementalOutput with CanHandleConstraints
    with CanHandleCatalogMetadata
    with HasEngineImplementation[DeltaLakeTableEngine] {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  val connection: Option[DeltaLakeTableConnection] = connectionId.map(c => getConnection[DeltaLakeTableConnection](c))

  // prepare final path and table
  @transient private var hadoopPathHolder: Path = _

  private val filetype: String = ".parquet"

  override protected def createEngines: Seq[DeltaLakeTableEngine] =
    DataObjectEngine.createEngines[DeltaLakeTableEngine, DeltaLakeTableDataObject](this, classOf[DeltaLakeTableDataObject])

  override protected def engineNotFoundHint: String =
    "Add module sdl-deltalake (classic Spark engine) or sdl-sparkconnect (Spark Connect engine) to the classpath."

  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"($id) DeltaTable ${table.fullName} does not exist, so path must be set or table should be managed (isManaged=true)")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = {
        if (thisIsTableExisting) new Path(engine.getTableLocation)
        else getAbsolutePath
      }

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if (thisIsTableExisting && path.isDefined) {
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
  if (table.db.isEmpty) {
    throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")
  }

  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.Merge).contains(saveMode), s"($id) Only saveMode Overwrite and Append supported for now.")

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    super.prepare
    require(isDbExisting, s"($id) DB ${table.getDbName} doesn't exist (needs to be created manually).")
    // engine-specific preparation, e.g. checking spark options and initializing external table path with classic Spark engine
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
    validateSchemaMin(df.schema, "write")
    validateSchemaHasPartitionCols(df.schema.columns, "write")
    validateSchemaHasPrimaryKeyCols(df.schema.columns, "write")
  }

  override def preWrite(implicit context: ActionPipelineContext): Unit = {
    super.preWrite
  }

  // Note that table metadata (table comment, column comments, primary key) is not applied here anymore.
  // It can only change when the configuration or the code changes, so it is applied at deployment time by
  // DataObjectSchemaExporter, see CanHandleCatalogMetadata.
  override def postWrite(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    super.postWrite(partitionValues)
  }

  /**
   * Writes DataFrame to HDFS/Parquet and creates DeltaLake table.
   */
  override def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    val writeEngine = engine(df.subFeedType)
    val metrics = writeEngine.writeDataFrame(df, partitionValues, saveModeOptions)
    // vacuum delta lake table
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
      HdfsUtil.listFiles(hadoopPath, recursive = true, filterFun = s => s.isDirectory || s.getPath.getName.endsWith(filetype))(filesystem).nonEmpty
    if (!hasFiles) {
      logger.warn(s"($id) No files found at $hadoopPath. Can not import any data.")
      require(!failIfFilesMissing, s"($id) failIfFilesMissing is enabled and no files to process have been found in $hadoopPath.")
    }
    hasFiles
  }

  /**
   * List partitions.
   * Note that we need a Spark SQL statement as there might be partition directories with no current data inside
   */
  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = engine.listPartitions

  /**
   * Note that we will not delete the whole partition but just the data of the partition because delta lake keeps history
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = engine.deletePartitions(partitionValues)

  override def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit = engine.movePartitions(partitionValues)

  override def dropTable(implicit context: ActionPipelineContext): Unit = engine.dropTable

  override def getStats(update: Boolean = false)(implicit context: ActionPipelineContext): Map[String, Any] = engine.getStats(update)

  override def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])(implicit context: ActionPipelineContext): Map[String, Map[String,Any]] = engine.getColumnStats(update, lastModifiedAt)

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
   * Return the last table version
   */
  override def getState: Option[String] = {
    incrementalOutputExpr
  }

  def prepareAndExecSql(sqlOpt: Option[String], configName: Option[String], partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    sqlOpt.foreach(stmt => SQLUtil.execSqlBasedOnTable(stmt, table, engine.sql, s"($id) "))
  }

  def getExistingPKConstraint(catalog: Option[String], schema: Option[String], tableName: String)(implicit context: ActionPipelineContext): Option[PrimaryKeyDefinition] = {
    val catalogConstraint = if (catalog.isEmpty) "" else f" and TABLE_CATALOG = '${catalog.get}'"
    val schemaConstraint = if (schema.isEmpty) "" else f" and TABLE_SCHEMA = '${schema.get}'"
    val baseQuery = f"select COLUMN_NAME, CONSTRAINT_NAME as PK_NAME from INFORMATION_SCHEMA.KEY_COLUMN_USAGE where TABLE_NAME = '$tableName'"
    val query = Seq(baseQuery, schemaConstraint, catalogConstraint).mkString.toLowerCase
    val rows = engine.sql(query).collect
    val (primaryKeyCols, primaryKeyName) = rows.foldLeft(Set[String](), Set[String]())((sets, row) => (sets._1 + row.getAs[String](0), sets._2 + row.getAs[String](1)))
    (primaryKeyCols.toList, primaryKeyName.toList) match {
      case (List(), _) => None
      case (cols, List()) => Some(PrimaryKeyDefinition(cols))
      case (_, pk) if pk.size > 1 => throw new SQLException(f"The $tableName returns more than one Primary Key: ${pk.mkString}")
      case (cols, pk) => Some(PrimaryKeyDefinition(cols, Some(pk.head)))
    }
  }

  def dropPrimaryKeyConstraint(tableName: String, constraintName: String)(implicit context: ActionPipelineContext): Unit = {
    val query = f"ALTER TABLE $tableName DROP CONSTRAINT $constraintName".toLowerCase
    SQLUtil.execSql(query, engine.sql, s"($id) ")
  }

  def createPrimaryKeyConstraint(tableName: String, constraintName: String, cols: Seq[String])(implicit context: ActionPipelineContext): Unit = {
    val query = f"ALTER TABLE $tableName ADD CONSTRAINT $constraintName PRIMARY KEY (${cols.mkString(",")}) RELY"
    SQLUtil.execSql(query, engine.sql, s"($id) ")
  }

  override def getTableComment(implicit context: ActionPipelineContext): Option[String] =
    CatalogMetadataSqlUtil.getTableComment(table, engine.sql)

  override def setTableComment(comment: String)(implicit context: ActionPipelineContext): Unit =
    CatalogMetadataSqlUtil.setTableComment(table, comment, engine.sql, s"($id) ")

  override def setColumnComments(comments: Map[Seq[String], String])(implicit context: ActionPipelineContext): Unit =
    CatalogMetadataSqlUtil.setColumnComments(table, comments, engine.sql, s"($id) ")
}

object DeltaLakeTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeltaLakeTableDataObject = {
    extract[DeltaLakeTableDataObject](config)
  }

  /**
   * Normalize delta operation metrics from DESCRIBE HISTORY / DeltaTable.history to standard SDLB metric names.
   * Shared by the engine implementations.
   */
  private[smartdatalake] def normalizeDeltaMetrics(operationMetrics: scala.collection.Map[String, String]): MetricsMap = {
    operationMetrics
      // normalize names lowercase with underscore
      .map { case (k, v) => (StringUtil.strCamelCase2LowerCaseWithUnderscores(k), Try(v.toLong).getOrElse(v)) }
      // standardize naming
      .map {
        case ("num_output_rows", v) => "rows_inserted" -> v
        case ("num_updated_rows", v) => "rows_updated" -> v
        case ("num_deleted_rows", v) => "rows_deleted" -> v
        case ("num_target_rows_inserted", v) => "rows_inserted" -> v
        case ("num_target_rows_updated", v) => "rows_updated" -> v
        case ("num_target_rows_deleted", v) => "rows_deleted" -> v
        case ("num_source_rows", v) => "records_written" -> v
        case (k, v) => k -> v
      }.toMap
  }
}

/**
 * SPI for engine-specific implementations of [[DeltaLakeTableDataObject]].
 *
 * Implementations are discovered on the classpath and must have a public constructor
 * with a single DeltaLakeTableDataObject parameter, see [[DataObjectEngine]].
 * The classic Spark implementation is provided by module sdl-deltalake,
 * the Spark Connect implementation by module sdl-sparkconnect.
 */
private[smartdatalake] trait DeltaLakeTableEngine extends DataObjectEngine {

  /**
   * Read the table, incl. incremental CDC read when incrementalOutputExpr is set (DataObjectStateIncrementalMode).
   */
  def getDataFrame(partitionValues: Seq[PartitionValues], incrementalOutputExpr: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame

  /**
   * Write/merge the DataFrame to the table, incl. NoDataToProcessWarning check and delta operation metrics.
   */
  def writeDataFrame(df: GenericDataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions])(implicit context: ActionPipelineContext): MetricsMap

  /**
   * Engine-specific preparation, e.g. checking spark options and repairing/registering an external table path (classic Spark only).
   */
  def prepare()(implicit context: ActionPipelineContext): Unit

  def vacuum()(implicit context: ActionPipelineContext): Unit

  def isDbExisting(implicit context: ActionPipelineContext): Boolean

  def isTableExisting(implicit context: ActionPipelineContext): Boolean

  def dropTable(implicit context: ActionPipelineContext): Unit

  def getTableLocation(implicit context: ActionPipelineContext): String

  def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues]

  def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit

  def movePartitions(partitionValues: Seq[(PartitionValues, PartitionValues)])(implicit context: ActionPipelineContext): Unit

  def getStats(update: Boolean)(implicit context: ActionPipelineContext): Map[String, Any]

  def getColumnStats(update: Boolean, lastModifiedAt: Option[Long])(implicit context: ActionPipelineContext): Map[String, Map[String, Any]]

  /**
   * Execute a SQL statement. Basis for generic SQL composition in the DataObject (e.g. constraints and comments).
   */
  def sql(stmt: String)(implicit context: ActionPipelineContext): GenericDataFrame
}
