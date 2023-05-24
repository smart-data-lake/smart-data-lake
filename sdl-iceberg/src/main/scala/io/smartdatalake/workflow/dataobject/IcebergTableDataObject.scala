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
import io.smartdatalake.config.SdlConfigObject.{ConnectionId, DataObjectId}
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions._
import io.smartdatalake.util.hdfs.HdfsUtil.RemoteIteratorWrapper
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues}
import io.smartdatalake.util.hive.HiveUtil
import io.smartdatalake.util.misc._
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.connection.IcebergTableConnection
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.iceberg.catalog.{Catalog, Namespace, TableIdentifier}
import org.apache.iceberg.spark.Spark3Util.{CatalogAndIdentifier, identifierToTableIdentifier}
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.{Spark3Util, SparkSchemaUtil, SparkWriteOptions}
import org.apache.iceberg.types.TypeUtil
import org.apache.iceberg.{PartitionSpec, TableProperties}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

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
 * - table is not registered and path does not exists -> table is created on write
 *
 * IcebergTableDataObject implements
 * - [[CanMergeDataFrame]] by writing a temp table and using one SQL merge statement.
 * - [[CanEvolveSchema]] by using internal Iceberg API.
 * - Overwriting partitions is implemented by using DataFrameWriterV2.overwrite(condition) API in one transaction.
 *
 * @param id unique name of this data object
 * @param path hadoop directory for this table. If it doesn't contain scheme and authority, the connections pathPrefix is applied.
 *             If pathPrefix is not defined or doesn't define scheme and authority, default schema and authority is applied.
 * @param partitions partition columns for this data object
 * @param options Options for Iceberg tables see: [[https://iceberg.apache.org/docs/latest/configuration/]]
 * @param schemaMin An optional, minimal schema that this DataObject must have to pass schema validation on reading and writing.
 *                  Define schema by using a DDL-formatted string, which is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param table Iceberg table to be written by this output
 * @param constraints List of row-level [[Constraint]]s to enforce when writing to this data object.
 * @param expectations List of [[Expectation]]s to enforce when writing to this data object. Expectations are checks based on aggregates over all rows of a dataset.
 * @param saveMode [[SDLSaveMode]] to use when writing files, default is "overwrite". Overwrite, Append and Merge are supported for now.
 * @param allowSchemaEvolution If set to true schema evolution will automatically occur when writing to this DataObject with different schema, otherwise SDL will stop with error.
 * @param historyRetentionPeriod Optional Iceberg retention threshold in hours. Files required by the table for reading versions younger than retentionPeriod will be preserved and the rest of them will be deleted.
 * @param acl override connection permissions for files created tables hadoop directory with this connection
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 * @param connectionId optional id of [[io.smartdatalake.workflow.connection.HiveTableConnection]]
 * @param metadata meta data
 */
case class IcebergTableDataObject(override val id: DataObjectId,
                                  path: Option[String],
                                  override val partitions: Seq[String] = Seq(),
                                  override val options: Map[String,String] = Map(),
                                  override val schemaMin: Option[GenericSchema] = None,
                                  override var table: Table,
                                  override val constraints: Seq[Constraint] = Seq(),
                                  override val expectations: Seq[Expectation] = Seq(),
                                  saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                                  override val allowSchemaEvolution: Boolean = false,
                                  historyRetentionPeriod: Option[Int] = None, // hours
                                  acl: Option[AclDef] = None,
                                  connectionId: Option[ConnectionId] = None,
                                  override val expectedPartitionsCondition: Option[String] = None,
                                  override val housekeepingMode: Option[HousekeepingMode] = None,
                                  override val metadata: Option[DataObjectMetadata] = None)
                                 (@transient implicit val instanceRegistry: InstanceRegistry)
  extends TransactionalTableDataObject with CanMergeDataFrame with CanEvolveSchema with CanHandlePartitions with HasHadoopStandardFilestore with ExpectationValidation {

  /**
   * Connection defines db, path prefix (scheme, authority, base path) and acl's in central location
   */
  private val connection = connectionId.map(c => getConnection[IcebergTableConnection](c))

  // prepare final path and table
  @transient private var hadoopPathHolder: Path = _

  val filetype: String = "." + options.getOrElse("write.format.default", "parquet") // Iceberg also supports avro or orc by setting this option, but default is parquet

  def hadoopPath(implicit context: ActionPipelineContext): Path = {
    implicit val session: SparkSession = context.sparkSession
    val thisIsTableExisting = isTableExisting
    require(thisIsTableExisting || path.isDefined, s"($id) Iceberg table ${table.fullName} does not exist, so path must be set.")

    if (hadoopPathHolder == null) {
      hadoopPathHolder = if (thisIsTableExisting) {
        new Path(getIcebergTable.location)
      } else getAbsolutePath

      // For existing tables, check to see if we write to the same directory. If not, issue a warning.
      if (thisIsTableExisting && path.isDefined) {
        // Normalize both paths before comparing them (remove tick / tock folder and trailing slash)
        val hadoopPathNormalized = HiveUtil.normalizePath(hadoopPathHolder.toString)
        val definedPathNormalized = HiveUtil.normalizePath(getAbsolutePath.toString)

        if (definedPathNormalized != hadoopPathNormalized)
          logger.warn(s"($id) Table ${table.fullName} exists already with different path $path. The table will use the existing path definition $hadoopPathHolder!")
      }
    }
    hadoopPathHolder
  }

  private def getAbsolutePath(implicit context: ActionPipelineContext) = {
    val prefixedPath = HdfsUtil.prefixHadoopPath(path.get, connection.map(_.pathPrefix))
    HdfsUtil.makeAbsolutePath(prefixedPath)(getFilesystem(prefixedPath, context.serializableHadoopConf)) // dont use "filesystem" to avoid loop
  }

  table = table.overrideCatalogAndDb(connection.flatMap(_.catalog), connection.map(_.db))
  if (table.db.isEmpty) throw ConfigurationException(s"($id) db is not defined in table and connection for dataObject.")

  // prepare tmp table used for merge statement
  private val tmpTable = {
    val tmpTableName = s"${table.name}_sdltmp"
    table.copy(name = tmpTableName)
  }

  assert(Seq(SDLSaveMode.Overwrite, SDLSaveMode.Append, SDLSaveMode.Merge).contains(saveMode), s"($id) Only saveMode Overwrite, Append and Merge supported for now.")

  def getMetadataPath(implicit context: ActionPipelineContext) = {
    options.get("write.metadata.path").map(new Path(_))
      .getOrElse(new Path(hadoopPath,"metadata"))
  }

  override def prepare(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    super.prepare
    if (connection.exists(_.checkIcebergSparkOptions)) {
      require(session.conf.getOption("spark.sql.extensions").toSeq.flatMap(_.split(',')).contains("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"),
        s"($id) Iceberg spark properties are missing. Please set spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions and org.apache.iceberg.spark.SparkSessionCatalog")
    }
    if(!isDbExisting) {
      // DB (schema) is created automatically by iceberg when creating tables. But we would like to keep the same behaviour as done by spark_catalog, where only default DB is existing, and others must be created manually.
      require(table.db.contains("default"), s"($id) DB ${table.db.get} doesn't exist (needs to be created manually).")
    }
    if (!isTableExisting) {
      require(path.isDefined, s"($id) If DeltaLake table does not exist yet, path must be set.")
      if (filesystem.exists(hadoopPath)) {
        if (filesystem.exists(getMetadataPath)) {
          // define an iceberg table, metadata can be read from files.
          getIcebergCatalog.registerTable(getTableIdentifier, getMetadataPath.toString)
          logger.info(s"($id) Creating Iceberg table ${table.fullName} for existing path $hadoopPath")
        } else {
          // if path has existing parquet files, convert to delta table
          require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no parquet files. Delete whole base path to reset Iceberg table.")
          convertPathToIceberg
        }
      }
    } else if (filesystem.exists(hadoopPath)) {
      if (!filesystem.exists(getMetadataPath)) {
        // if path has existing parquet files but not in delta format, convert to delta format
        require(checkFilesExisting, s"($id) Path $hadoopPath exists but contains no parquet files. Delete whole base path to reset Iceberg table.")
        convertTableToIceberg
        logger.info(s"($id) Converted existing table ${table.fullName} to Iceberg table")
      }
    } else {
      dropTable
      logger.info(s"($id) Dropped existing Iceberg table ${table.fullName} because path was missing")
    }
    filterExpectedPartitionValues(Seq()) // validate expectedPartitionsCondition
  }

  /**
   * converts an existing hive table with parquet files to an iceberg table
   */
  private[smartdatalake] def convertTableToIceberg(implicit context: ActionPipelineContext): Unit = {
    SparkActions.get(context.sparkSession).migrateTable(getIdentifier.toString)
  }

  /**
   * converts an existing path with parquet files to an iceberg table
   */
  private[smartdatalake] def convertPathToIceberg(implicit context: ActionPipelineContext): Unit = {
    val existingDf = context.sparkSession.read.parquet(hadoopPath.toString)
    val schema = SparkSchemaUtil.convert(existingDf.schema)
    val partitionSpec = partitions.foldLeft(PartitionSpec.builderFor(schema)){
      case (partitionSpec, colName) => partitionSpec.identity(colName)
    }.build
    getIcebergCatalog.createTable(getTableIdentifier, schema, partitionSpec, hadoopPath.toString, options.asJava)
    context.sparkSession.sql(s"CALL ${getIcebergCatalog.name}.system.add_files(table => '${getIdentifier.toString}', source_table => '`parquet`.`$hadoopPath`')")
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
    if (isTableExisting) {
      val saveModeTargetDf = saveModeOptions.map(_.convertToTargetSchema(df)).getOrElse(df)
      val existingSchema = SparkSchema(getSparkDataFrame().schema)
      if (allowSchemaEvolution && existingSchema.equalsSchema(SparkSchema(saveModeTargetDf.schema))) evolveTableSchema(saveModeTargetDf.schema)
      else validateSchema(SparkSchema(saveModeTargetDf.schema), existingSchema, "write")
    }
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
   * Writes DataFrame to HDFS/Parquet and creates Iceberg table.
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
    // V1 writer is needed to create external table
    val dfWriter = saveModeTargetDf.write
      .format("iceberg")
      .options(options)
      .option("path", hadoopPath.toString)
    if (isTableExisting) {
      // check schema
      if (!allowSchemaEvolution) validateSchema(SparkSchema(saveModeTargetDf.schema), SparkSchema(session.table(table.fullName).schema), "write")
      // update table property "spark.accept-any-schema" to allow schema evolution. This disables Spark schema checks as they dont allow missing columns. Iceberg schema checks still apply.
      updateTableProperty(TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA, allowSchemaEvolution.toString, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT.toString)
      // apply
      if (finalSaveMode == SDLSaveMode.Merge) {
        // merge operations still need all columns for potential insert/updateConditions. Therefore dfPrepared instead of saveModeTargetDf is passed on.
        mergeDataFrameByPrimaryKey(dfPrepared, saveModeOptions.map(SaveModeMergeOptions.fromSaveModeOptions).getOrElse(SaveModeMergeOptions()))
      } else {
        // V2 writer can be used if table is existing, it supports overwriting given partitions
        val dfWriterV2 = saveModeTargetDf
          .writeTo(table.fullName)
          .option(SparkWriteOptions.MERGE_SCHEMA, allowSchemaEvolution.toString)
        if (partitions.isEmpty) {
          SDLSaveMode.execV2(finalSaveMode, dfWriterV2, partitionValues)
        } else {
          if (finalSaveMode == SDLSaveMode.Overwrite && partitionValues.isEmpty) {
            throw new ProcessingLogicException(s"($id) Overwrite without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
          }
          SDLSaveMode.execV2(finalSaveMode, dfWriterV2, partitionValues)
        }
      }
    } else {
      if (partitions.isEmpty) {
        dfWriter.saveAsTable(table.fullName)
      } else {
        dfWriter
          .partitionBy(partitions: _*)
          .saveAsTable(table.fullName)
      }
    }

    // vacuum delta lake table
    vacuum

    // fix acls
    if (acl.isDefined) AclUtil.addACLs(acl.get, hadoopPath)(filesystem)
  }

  private def writeToTempTable(df: DataFrame)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    // check if temp-table existing
    if(getIcebergCatalog.tableExists(getTableIdentifier)) {
      logger.error(s"($id) Temporary table ${tmpTable.fullName} for merge already exists! There might be a potential conflict with another job. It will be replaced.")
    }
    // write to temp-table
    df.write
      .format("iceberg")
      .option("path", hadoopPath.toString+"_sdltmp")
      .saveAsTable(tmpTable.fullName)
  }

  /**
   * Merges DataFrame with existing table data by writing DataFrame to a temp-table and using SQL Merge-statement.
   * Table.primaryKey is used as condition to check if a record is matched or not. If it is matched it gets updated (or deleted), otherwise it is inserted.
   * This all is done in one transaction.
   */
  def mergeDataFrameByPrimaryKey(df: DataFrame, saveModeOptions: SaveModeMergeOptions)(implicit context: ActionPipelineContext): Unit = {
    implicit val session: SparkSession = context.sparkSession
    assert(table.primaryKey.exists(_.nonEmpty), s"($id) table.primaryKey must be defined to use mergeDataFrameByPrimaryKey")

    try {
      // write data to temp table
      writeToTempTable(df)
      // override missing columns with null value, as Iceberg needs all target columns be included in insert statement
      val targetCols = session.table(table.fullName).schema.fieldNames
      val missingCols = targetCols.diff(df.columns)
      val saveModeOptionsExt = saveModeOptions.copy(
        insertValuesOverride = saveModeOptions.insertValuesOverride ++ missingCols.map(_ -> "null"),
        updateColumns = if (saveModeOptions.updateColumns.isEmpty) df.columns.diff(table.primaryKey.get) else saveModeOptions.updateColumns
      )
      // prepare SQL merge statement
      // note that we pass all target cols instead of new df columns as parameter, but with customized saveModeOptionsExt
      val mergeStmt = SQLUtil.createMergeStatement(table, targetCols, tmpTable.fullName, saveModeOptionsExt, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
      // execute
      logger.info(s"($id) executing merge statement with options: ${ProductUtil.attributesWithValuesForCaseClass(saveModeOptionsExt).map(e => e._1+"="+e._2).mkString(" ")}")
      logger.debug(s"($id) merge statement: $mergeStmt")
      context.sparkSession.sql(mergeStmt)
    } finally {
      // cleanup temp table
      val tmpTableIdentifier = TableIdentifier.of((getIdentifier.namespace :+ tmpTable.name):_*)
      getIcebergCatalog.dropTable(tmpTableIdentifier)
    }
  }

  def updateTableProperty(name: String, value: String, default: String)(implicit context: ActionPipelineContext) = {
    val currentValue = getIcebergTable.properties.asScala.getOrElse(name, default)
    if (currentValue != value) {
      getIcebergTable.updateProperties.set(name, value).commit
    }
    logger.info(s"($id) updated Iceberg table property $name to $value")
  }

  /**
   * Iceberg as a write option 'mergeSchema' (see also SparkWriteOptions.MERGE_SCHEMA),
   * but it doesnt work as there is another validation before that checks the schema (e.g. QueryCompilationErrors$.cannotWriteTooManyColumnsToTableError in the stack trace)
   * This code is therefore copied from SparkWriteBuilder.validateOrMergeWriteSchema:246ff
   */
  def evolveTableSchema(dsSchema: StructType)(implicit context: ActionPipelineContext): Unit = {
    logger.info(s"($id) evolving Iceberg table schema")
    val table = getIcebergTable
    val caseSensitive = Environment.caseSensitive

    // convert the dataset schema and assign fresh ids for new fields
    val newSchema = SparkSchemaUtil.convertWithFreshIds(table.schema, dsSchema, caseSensitive)

    // update the table to get final id assignments and validate the changes
    val update = table.updateSchema.caseSensitive(caseSensitive).unionByNameWith(newSchema)
    val mergedSchema = update.apply

    // reconvert the dsSchema without assignment to use the ids assigned by UpdateSchema
    val writeSchema = SparkSchemaUtil.convert(mergedSchema, dsSchema, caseSensitive)

    TypeUtil.validateWriteSchema(mergedSchema, writeSchema, false, false)

    // if the validation passed, update the table schema
    update.commit()
  }

  def vacuum(implicit context: ActionPipelineContext): Unit = {
    historyRetentionPeriod.foreach { hours =>
      val (_, d) = PerformanceUtils.measureDuration {
        SparkActions.get(context.sparkSession)
          .expireSnapshots(getIcebergTable)
          .expireOlderThan(System.currentTimeMillis - hours * 60 * 60 * 1000)
          .execute
      }
      logger.info(s"($id) vacuum took $d")
    }
  }

  def getIcebergCatalog(implicit context: ActionPipelineContext): Catalog = {
    getSparkCatalog.icebergCatalog
  }
  def getSparkCatalog(implicit context: ActionPipelineContext): TableCatalog with SupportsNamespaces with HasIcebergCatalog = {
    getCatalogAndIdentifier.catalog match {
      case c: TableCatalog with HasIcebergCatalog with SupportsNamespaces => c
      case c => throw new IllegalStateException(s"($id) ${c.name}:${c.getClass.getSimpleName} is not a TableCatalog with SupportsNamespaces with HasIcebergCatalog implementation")
    }
  }
  def getIdentifier(implicit context: ActionPipelineContext): Identifier = {
    getCatalogAndIdentifier.identifier
  }
  def getTableIdentifier(implicit context: ActionPipelineContext): TableIdentifier = {
    convertToTableIdentifier(getIdentifier)
  }
  def convertToTableIdentifier(identifier: Identifier): TableIdentifier = {
    TableIdentifier.of(Namespace.of(identifier.namespace:_*), identifier.name)
  }
  private def getCatalogAndIdentifier(implicit context: ActionPipelineContext): CatalogAndIdentifier = {
    if (_catalogAndIdentifier.isEmpty) {
      _catalogAndIdentifier = Some(Spark3Util.catalogAndIdentifier(context.sparkSession, table.nameParts.asJava))
    }
    _catalogAndIdentifier.get
  }
  private var _catalogAndIdentifier: Option[CatalogAndIdentifier] = None

  def getIcebergTable(implicit context: ActionPipelineContext) = {
    // Note: loadTable is cached by default in Iceberg catalog
    getIcebergCatalog.loadTable(identifierToTableIdentifier(getIdentifier))
  }

  override def isDbExisting(implicit context: ActionPipelineContext): Boolean = {
    getSparkCatalog.namespaceExists(Array(table.db.get))
  }

  override def isTableExisting(implicit context: ActionPipelineContext): Boolean = {
    getIcebergCatalog.tableExists(identifierToTableIdentifier(getIdentifier))
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

  override def listPartitions(implicit context: ActionPipelineContext): Seq[PartitionValues] = {
    val partitionsDf = context.sparkSession.sql(s"select partition.* from ${table.toString}.partitions")
    val partitions = partitionsDf.collect.toSeq.map(r => r.getValuesMap[Any](partitionsDf.columns).mapValues(_.toString))
    partitions.map(PartitionValues(_))
  }

  /**
   * Note that Iceberg will not delete the whole partition but just the data of the partition because Iceberg keeps history
   */
  override def deletePartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    val deleteStmt = SQLUtil.createDeletePartitionStatement(table.fullName, partitionValues, SQLUtil.sparkQuoteCaseSensitiveColumn(_))
    context.sparkSession.sql(deleteStmt)
  }

  override def dropTable(implicit context: ActionPipelineContext): Unit = {
    getIcebergCatalog.dropTable(getTableIdentifier, true) // purge
    HdfsUtil.deletePath(hadoopPath, false)(filesystem)
  }

  override def factory: FromConfigFactory[DataObject] = IcebergTableDataObject

}

object IcebergTableDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): IcebergTableDataObject = {
    extract[IcebergTableDataObject](config)
  }
}

