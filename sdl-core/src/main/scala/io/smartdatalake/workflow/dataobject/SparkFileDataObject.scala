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

import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeOptions}
import io.smartdatalake.metrics.SparkStageMetricsListener
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.{CompactionUtil, EnvironmentUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.CollectSetDeterministic.collect_set_deterministic
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.util.spark.DataFrameUtil.{DataFrameReaderUtils, DataFrameWriterUtils}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkObservation, SparkSchema, SparkSimpleDataType, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{DataSource, FileScanRDD}
import org.apache.spark.sql.functions.{col, input_file_name, lit}
import org.apache.spark.sql.types.{DataType, StringType, StructType}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import scala.collection.mutable
import scala.reflect.runtime.universe.typeOf
import scala.util.Try

/**
 * A [[DataObject]] backed by a file in HDFS. Can load file contents into an Apache Spark [[DataFrame]]s.
 *
 * Delegates read and write operations to Apache Spark [[DataFrameReader]] and [[DataFrameWriter]] respectively.
 */
@DeveloperApi
trait SparkFileDataObject extends HadoopFileDataObject
  with CanCreateSparkDataFrame with CanCreateStreamingDataFrame
  with CanWriteSparkDataFrame with CanCreateIncrementalOutput
  with UserDefinedSchema with SchemaValidation with SmartDataLakeLogger {

  /**
   * The Spark-Format provider to be used
   */
  def format: String

  /**
   * Hook to use different Spark-Format provider for reading
   */
  def readFormat: String = format // hook to use different provider for reading

  /**
   * The name of the (optional) additional column containing the source filename
   */
  def filenameColumn: Option[String]

  /**
   * Definition of repartition operation before writing DataFrame with Spark to Hadoop.
   */
  def sparkRepartition: Option[SparkRepartitionDef]

  /**
   * Callback that enables potential transformation to be applied to `df` before the data is written.
   *
   * Default is to validate the `schemaMin` and not apply any modification.
   */
  def beforeWrite(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    validateSchemaMin(SparkSchema(df.schema), "write")
    validateSchemaHasPartitionCols(df, "write")
    getSchema.foreach(schemaExpected => validateSchema(SparkSchema(df.schema), schemaExpected, "write"))
    df
  }

  /**
   * Callback that enables potential transformation to be applied to `df` after the data is read.
   *
   * Default is to validate the `schemaMin` and not apply any modification.
   */
  def afterRead(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    validateSchemaMin(SparkSchema(df.schema), "read")
    validateSchemaHasPartitionCols(df, "read")
    getSchema.map(createReadSchema).foreach(schemaExpected => validateSchema(SparkSchema(df.schema), schemaExpected, "read"))
    df
  }

  /**
   * Returns the user-defined schema for reading from the data source. By default, this should return `schema` but it
   * may be customized by data objects that have a source schema and ignore the user-defined schema on read operations.
   *
   * If a user-defined schema is returned, it overrides any schema inference. If no user-defined schema is set, the
   * schema may be inferred depending on the configuration and type of data frame reader.
   *
   * @return The consolidated schema to validate the schema on read and write, and for the data frame reader when reading from the source.
   */
  def getSchema(implicit context: ActionPipelineContext): Option[SparkSchema] = {
    _schemaHolder = _schemaHolder.orElse(
        // get defined schema, add potentially missing partition columns as type string
        schema.map(_.convert(typeOf[SparkSubFeed]))
          .map(schema => addPartitionCols(schema.asInstanceOf[SparkSchema]))
      )
      .orElse (
        // or try reading schema file
        if (filesystem.exists(schemaFile)) {
          val schemaContent = HdfsUtil.readHadoopFile(schemaFile)(filesystem)
          Some(SparkSchema(DataType.fromJson(schemaContent).asInstanceOf[StructType]))
        } else None
      )
      .orElse(
        // or try inferring schema from sample data file
        if (filesystem.exists(sampleFile)) {
          logger.info(s"($id) Inferring schema from sample data file")
          Some(inferSchemaFromPath(sampleFile.toString))
        } else None
      )
      .orElse(
        // use schemaMin if defined
        schemaMin.map(_.convert(typeOf[SparkSubFeed]))
          .map(schema => addPartitionCols(schema.asInstanceOf[SparkSchema]))
      )
    // return
    _schemaHolder
  }
  private var _schemaHolder: Option[SparkSchema] = None
  private def schemaFile(implicit context: ActionPipelineContext) = {
    val fileStat = Try(filesystem.getFileStatus(hadoopPath)).toOption
    val dataObjectRootPath = if (fileStat.exists(_.isFile)) hadoopPath.getParent else hadoopPath
    new Path( new Path(dataObjectRootPath, ".schema"), "currentSchema.json")
  }
  protected def inferSchemaFromPath(path: String)(implicit context: ActionPipelineContext): SparkSchema = {
    import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
    val df = context.sparkSession.read
      .format(readFormat)
      .options(options)
      .load(path)
      .withOptionalColumn(filenameColumn, input_file_name())
    val dfWithPartitions = partitions.foldLeft(df) {
      case (df, p) => df.withColumn(p, functions.lit("dummyString"))
    }
    SparkSchema(dfWithPartitions.schema)
  }

  private def addPartitionCols(schema: SparkSchema): SparkSchema = {
    val missingPartitions = partitions.filterNot(schema.columnExists)
    if (missingPartitions.nonEmpty) {
      logger.info(s"($id) adding missing partition columns ${missingPartitions.mkString(", ")} to schema")
      missingPartitions.foldLeft(schema) { case (s, c) => s.add(c, SparkSimpleDataType(StringType)) }
    } else schema
  }

  /**
   * Provide a sample data file name to be created by file-based Action. If none is returned, no file is created.
   */
  override def createSampleFile(implicit context: ActionPipelineContext): Option[String] = {
    // only create new sample file if there is no schema defined, and no schema file exists or an update is forced by the environment configuration
    if (schema.isEmpty && (Environment.updateSparkFileDataObjectSampleDataFile || !filesystem.exists(sampleFile))) {
      Some(sampleFile.toString)
    } else None
  }
  private def sampleFile(implicit context: ActionPipelineContext) = new Path( new Path(hadoopPath, ".sample"), s"sampleData.${fileName.split('.').last.filter(_ != '*')}")

  /**
   * Hook for subclasses to ignore schema when calling Spark reader.
   */
  protected def ignoreSchemaForReader: Boolean = false

  /**
   * Hook for subclasses to switch to reading files one by one with Spark, as some DataSources dont support reading folders, e.g. spark-excel.
   */
  protected def handleFilesOneByOne: Boolean = false

  override def options: Map[String, String] = Map() // override options because of conflicting definitions in CanCreateSparkDataFrame and CanWriteSparkDataFrame

  /**
   * Hook to use different options for reading
   */
  protected def readOptions: Map[String, String] = options // hook to use different provider for reading

  /**
   * Constructs an Apache Spark [[DataFrame]] from the underlying file content.
   *
   * @see [[DataFrameReader]]
   * @return a new [[DataFrame]] containing the data stored in the file at `path`
   */
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    import io.smartdatalake.util.spark.DataFrameUtil.DfSDL

    val wrongPartitionValues = PartitionValues.checkWrongPartitionValues(partitionValues, partitions)
    assert(wrongPartitionValues.isEmpty, s"getDataFrame got request with PartitionValues keys ${wrongPartitionValues.mkString(",")} not included in $id partition columns ${partitions.mkString(", ")}")

    val schemaOpt = getSchema.map(_.inner)
    if (schemaOpt.isEmpty && !checkFilesExisting) {
      //without either schema or data, no data frame can be created
      require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined if there are no existing files.")
    }

    // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
    if (!filesystem.exists(hadoopPath)) filesystem.mkdirs(hadoopPath)

    // Prepare incremental output in exec phase
    val incrementalOutputOptions = if (context.isExecPhase) getIncrementalOutputOptions
    else Map[String,String]()

    // get and customize content
    var df = if (handleFilesOneByOne) getContentFilesOneByOne(partitionValues, schemaOpt.filter(_ => !ignoreSchemaForReader), incrementalOutputOptions)
    else if (isV2ReadDataSource) getContentV2(partitionValues, schemaOpt.filter(_ => !ignoreSchemaForReader), incrementalOutputOptions)
    else getContentV1(partitionValues, schemaOpt.filter(_ => !ignoreSchemaForReader), incrementalOutputOptions)
    df = customizeContent(df)

    // early check for no data to process.
    // This also prevents an error on Databricks when using filesObserver if there are no files to process. See also [[CollectSetDeterministic]].
    if (context.isExecPhase && Environment.enableSparkFileDataObjectNoDataCheck && SparkFileDataObject.tryGetFilesProcessedFromSparkPlan(id.id, df).exists(_.isEmpty))
      throw NoDataToProcessWarning(id.id, s"($id) No files to process found in execution plan")

    // add filename column
    df = df.withOptionalColumn(filenameColumn, input_file_name())

    // configure observer to get files processed for incremental execution mode
    if (filesObservers.nonEmpty && context.isExecPhase) {
      df = configureObservers(df)
    }

    // finalize & return DataFrame
    afterRead(df)
  }

  /**
   * Update incremental output state and prepare options for filtering DataSource.
   */
  protected def getIncrementalOutputOptions: Map[String,String] = {
    incrementalOutputState.map { previousOutputState =>
      // Comparison of modifiedAfter and modifiedBefore are both exclusive on Microsecond level, but file timestamps maximum detail is milliseconds.
      // Actually comparison of one operator should be inclusive to avoid reading files in edge cases.
      // Current timestamp is also at millisecond level. If we subtract one microsecond from current timestamp we can avoid the problems because of exclusive comparison.
      incrementalOutputState = Some(LocalDateTime.now.minusNanos(1000))
      val dateFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSS")
      logger.info(s"($id) incremental output selected files with modification date greater than ${dateFormatter.format(previousOutputState)} and smaller than ${dateFormatter.format(incrementalOutputState.get)}")
      Map(
        "modifiedAfter" -> dateFormatter.format(fixWindowsTimezone(previousOutputState)),
        "modifiedBefore" -> dateFormatter.format(fixWindowsTimezone(incrementalOutputState.get))
      )
    }.getOrElse(Map[String, String]())
  }

  /**
   * Hook for subclasses to customize content on read. Default 1:1.
   */
  protected def customizeContent(df: DataFrame)(implicit context: ActionPipelineContext) = df

  /**
   * Prepares the DataFrame with the content when reading.
   * Uses Spark DataSource V2 interface.
   */
  protected def getContentV2(partitionValues: Seq[PartitionValues], schema: Option[StructType], incrementalOutputOptions: Map[String,String])(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    if (partitions.isEmpty || partitionValues.isEmpty) {
      session.read
        .format(readFormat)
        .options(readOptions ++ incrementalOutputOptions)
        .optionalSchema(schema)
        .load(hadoopPath.toString)
    } else {
      val reader = session.read
        .format(readFormat)
        .options(readOptions ++ incrementalOutputOptions)
        .optionalSchema(schema)
        .option("basePath", hadoopPath.toString) // this is needed for partitioned tables when subdirectories are read directly; it then keeps the partition columns from the subdirectory path in the dataframe
      val pathsToRead = partitionValues.flatMap(getConcreteInitPaths).map(_.toString)
      val df = if (pathsToRead.nonEmpty) Some(reader.load(pathsToRead: _*)) else None
      df.filter(df => schema.isDefined || partitions.diff(df.columns).isEmpty) // filter DataFrames without partition columns as they are empty (this might happen if there is no schema specified and the partition is empty)
        .getOrElse {
          // if there are no paths to read for given partition values, handle no data
          if (context.isExecPhase) {
            // skip action in exec phase
            throw NoDataToProcessWarning(id.id, s"($id) No existing files found for partition values ${partitionValues.mkString(", ")}.")
          } else {
            // create empty data frame in init phase
            require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined as there are no existing files for partition values ${partitionValues.mkString(", ")}.")
            DataFrameUtil.getEmptyDataFrame(schema.get)
          }
        }
    }
  }

  /**
   * Prepares the DataFrame with the content when reading.
   * Uses Spark DataSource V1 interface. V1 interface has limited support for reading partitioned data.
   * getContentV1 implements an approach to query a DataFrame per partition, adding partition columns and union all DataFrames.
   */
  protected def getContentV1(partitionValues: Seq[PartitionValues], schema: Option[StructType], incrementalOutputOptions: Map[String,String])(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    if (partitions.isEmpty) {
      session.read
        .format(readFormat)
        .options(readOptions ++ incrementalOutputOptions)
        .optionalSchema(schema)
        .option("path", hadoopPath.toString) // spark-xml is a V1 source and only supports one path, which must be given as option...
        .load()
    } else {
      val schemaWithoutPartitions = schema.map(s => StructType(s.filterNot(f => partitions.contains(f.name))))
      val reader = session.read
        .format(readFormat)
        .options(readOptions ++ incrementalOutputOptions)
        .optionalSchema(schemaWithoutPartitions)
      val partitionValuesToRead = if (partitionValues.nonEmpty) partitionValues else listPartitions
      val pathsToRead = partitionValuesToRead.flatMap(pv => getConcreteFullPaths(pv).map(p => (extractPartitionValuesFromDirPath(p.toString), p)))
        .filter{case (_,path) => filesystem.globStatus(new Path(path,fileName)).nonEmpty} // filter empty path to avoid NullPointerException in DataFrame
      val df = if (pathsToRead.nonEmpty) Some(
        pathsToRead.map { case (pv, path) =>
          partitions.foldLeft(reader.option("path", path.toString).load()) { // spark-xml is a V1 source and only supports one path, which must be given as option...
            case (df, partition) => df.withColumn(partition, lit(pv(partition).toString))
          }
        }.reduce(_ unionByName _)
      ) else None
      df.filter(df => schema.isDefined || partitions.diff(df.columns).isEmpty) // filter DataFrames without partition columns as they are empty (this might happen if there is no schema specified and the partition is empty)
        .getOrElse {
          // if there are no paths to read for given partition values, handle no data
          if (context.isExecPhase) {
            // skip action in exec phase
            throw NoDataToProcessWarning(id.id, s"($id) No existing files found for partition values ${partitionValues.mkString(", ")}.")
          } else {
            // create empty data frame in init phase
            require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined as there are no existing files for partition values ${partitionValues.mkString(", ")}.")
            DataFrameUtil.getEmptyDataFrame(schema.get)
          }
        }
    }
  }

  /**
   * Prepares the DataFrame with the content when reading.
   * There are some DataSources which dont support reading multiple files, e.g. ExcelFileDataObject.
   * getContentFilesOneByOne implements an approach to query a DataFrame per file, adding partition columns and union all DataFrames.
   */
  protected def getContentFilesOneByOne(partitionValues: Seq[PartitionValues], schema: Option[StructType], incrementalOutputOptions: Map[String,String])(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    // search files to be read
    val files = if (filesystem.getFileStatus(hadoopPath).isFile) Seq((PartitionValues(Map()),hadoopPath))
    else  if (partitions.isEmpty) {
      filesystem.globStatus(new Path(hadoopPath,fileName)).toSeq
        .filter(_.isFile).map(fs => (PartitionValues(Map()),fs.getPath))
    } else {
      val pvs = if (partitionValues.isEmpty) listPartitions else partitionValues
      pvs.flatMap(pv => getConcreteFullPaths(pv, returnFiles = true).map(p => (extractPartitionValuesFromFilePath(p.toString),p)))
    }
    // get and union DataFrames per File
    val schemaWithoutPartitions = schema.map(s => StructType(s.filterNot(f => partitions.contains(f.name))))
    val reader = session.read
      .format(readFormat)
      .options(readOptions ++ incrementalOutputOptions)
      .optionalSchema(schemaWithoutPartitions)
    val df = if (files.nonEmpty) Some(
      files.map { case (pv,p) =>
        partitions.foldLeft(reader.option("path", p.toString).load()) {
          case (df, partition) => df.withColumn(partition, lit(pv(partition).toString))
        }
      }.reduce(_ unionByName _)
    ) else None
    df.getOrElse {
      // if there are no paths to read for given partition values, handle no data
      if (context.isExecPhase) {
        // skip action in exec phase
        throw NoDataToProcessWarning(id.id, s"($id) No existing files found for partition values ${partitionValues.mkString(", ")}.")
      } else {
        // create empty data frame in init phase
        require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined as there are no existing files for partition values ${partitionValues.mkString(", ")}.")
        DataFrameUtil.getEmptyDataFrame(schema.get)
      }
    }
  }

  /**
   * configure filename observer
   */
  protected def configureObservers(dfInput: DataFrame): DataFrame = {
    var df = dfInput
    if (filesObservers.size > 1) logger.warn(s"($id) files observation is not yet well supported when using from multiple actions in parallel")
    // force creating filenameColumn, and drop the it later again
    val forcedFilenameColumn = "__filename"
    if (filenameColumn.isEmpty) df = df.withColumn(forcedFilenameColumn, input_file_name())
    // initialize observers
    df = filesObservers.foldLeft(df) {
      case (df, (actionId, observer)) => observer.on(df, filenameColumn.getOrElse(forcedFilenameColumn))
    }
    filesObservers.clear()
    // drop forced filenameColumn
    if (filenameColumn.isEmpty) df = df.drop(forcedFilenameColumn)
    // return
    df
  }


  /**
   * It seems that Hadoop on Windows returns modified date in local timezone, but according to documentation it should be in UTC.
   * This results in wrong comparison of modified date by Spark, as Spark adds an additional local timezone offset to the files modification date.
   * To fix this we need to add an additional local timezone offset to the comparison thresholds given to spark.
   */
  private def fixWindowsTimezone(localDateTime: LocalDateTime): LocalDateTime = {
    if (EnvironmentUtil.isWindowsOS) LocalDateTime.ofInstant(localDateTime.atOffset(ZoneOffset.UTC).toInstant, ZoneId.systemDefault())
    else localDateTime
  }

  // Store incremental output state. It is stored as LocalDateTime because Spark options need local timezone.
  private var incrementalOutputState: Option[LocalDateTime] = None

  /**
   * Set timestamp for incremental output
   */
  override def setState(state: Option[String])(implicit context: ActionPipelineContext): Unit = {
    incrementalOutputState = state.map(LocalDateTime.parse)
      .orElse(Some(LocalDateTime.ofInstant(Instant.EPOCH, ZoneId.systemDefault)))
  }

  /**
   * Get timestamp of incremental output for saving to state
   */
  override def getState: Option[String] = {
    incrementalOutputState.map(_.toString)
  }

  // Store files observation object between call to setupFilesObserver until it is used in getSparkDataFrame.
  @transient private val filesObservers: mutable.Map[ActionId, ExecutionPlanSparkFilenameObservation[Row]] = mutable.Map()

  /**
   * Setup an observation of files processed through custom metrics.
   * This is used for incremental processing to keep track of files processed.
   * Note that filenameColumn needs to be configured for the DataObject in order for this to work.
   */
  def setupFilesObserver(actionId: ActionId): SparkFilenameObservation[Row] = {
    logger.debug(s"($id) setting up files observer for $actionId")
    // return existing observation for this action if existing, otherwise create a new one.
    filesObservers.getOrElseUpdate(actionId, new ExecutionPlanSparkFilenameObservation(actionId.id + "/" + id.id))
  }

  override def getStreamingDataFrame(options: Map[String, String], pipelineSchema: Option[StructType])(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    require(schema.orElse(pipelineSchema).isDefined, s"($id}) Schema must be defined for streaming SparkFileDataObject")
    // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
    if (!filesystem.exists(hadoopPath.getParent)) filesystem.mkdirs(hadoopPath)

    val schemaOpt = getSchema.map(_.inner).orElse(pipelineSchema).get
    val df = session.readStream
      .format(readFormat)
      .options(readOptions ++ this.options)
      .schema(schemaOpt)
      .load(hadoopPath.toString)

    afterRead(df)
  }

  override def createReadSchema(writeSchema: GenericSchema)(implicit context: ActionPipelineContext): GenericSchema = {
    val functions = DataFrameSubFeed.getFunctions(writeSchema.subFeedType)
    // add additional columns created by SparkFileDataObject
    filenameColumn.map(colName => addFieldIfNotExisting(writeSchema, colName, functions.stringType))
      .getOrElse(writeSchema)
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    // validate schema
    validateSchemaMin(SparkSchema(df.schema), "write")
    schema.foreach(schemaExpected => validateSchema(SparkSchema(df.schema), schemaExpected, "write"))
    // update current schema storage - this is to avoid schema inference and remember the schema if there is no data.
    if (!schema.isDefined && !context.simulation) createSchemaFile(df)
  }

  private def createSchemaFile(df: DataFrame)(implicit context: ActionPipelineContext): Unit = {
    if(Environment.updateSparkFileDataObjectSchemaFile || !filesystem.exists(schemaFile)) {
      logger.info(s"($id) Writing schema file")
      HdfsUtil.writeHadoopFile(schemaFile, df.schema.prettyJson)(filesystem)
    }
  }

  /**
   * Writes the provided [[DataFrame]] to the filesystem.
   *
   * The `partitionValues` attribute is used to partition the output by the given columns on the file system.
   *
   * @see [[DataFrameWriter.partitionBy]]
   * @param df the [[DataFrame]] to write to the file system.
   * @param partitionValues The partition layout to write.
   */
  final override def writeSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): MetricsMap = {
    implicit val session: SparkSession = context.sparkSession
    require(!isRecursiveInput, "($id) SparkFileDataObject cannot write dataframe when dataobject is also used as recursive input ")

    // prepare data
    var dfPrepared = beforeWrite(df)
    dfPrepared = sparkRepartition.map(_.prepareDataFrame(dfPrepared, partitions, partitionValues, id))
      .getOrElse(dfPrepared)

    // apply special save modes
    val finalSaveMode = saveModeOptions.map(_.saveMode).getOrElse(saveMode)
    finalSaveMode match {
      case SDLSaveMode.Overwrite =>
        if (partitionValues.nonEmpty) { // delete concerned partitions if existing, as Spark dynamic partitioning doesn't delete empty partitions
          deletePartitions(filterPartitionsExisting(partitionValues))
        } else {
          // SDLSaveMode.Overwrite: Workaround ADLSv2: overwrite unpartitioned data object as it is not deleted by spark csv writer (strangely it works for parquet)
          if (Environment.enableOverwriteUnpartitionedSparkFileDataObjectAdls) {
            deleteAll
          }
        }
      case SDLSaveMode.OverwriteOptimized =>
        if (partitionValues.nonEmpty) { // delete concerned partitions if existing, as append mode is used later
          deletePartitions(filterPartitionsExisting(partitionValues))
        } else if (partitions.isEmpty || context.globalConfig.allowOverwriteAllPartitionsWithoutPartitionValues.contains(id)) { // delete all data if existing, as append mode is used later
          deleteAll
        } else {
          throw new ProcessingLogicException(s"($id) OverwriteOptimized without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
        }
      case SDLSaveMode.OverwritePreserveDirectories => // only delete files but not directories
        if (partitionValues.nonEmpty) { // delete concerned partitions files if existing, as append mode is used later
          deletePartitionsFiles(filterPartitionsExisting(partitionValues))
        } else if (partitions.isEmpty || context.globalConfig.allowOverwriteAllPartitionsWithoutPartitionValues.contains(id)) { // delete all data if existing, as append mode is used later
          deleteAllFiles(hadoopPath)
        } else {
          throw new ProcessingLogicException(s"($id) OverwritePreserveDirectories without partition values is not allowed on a partitioned DataObject. This is a protection from unintentionally deleting all partition data.")
        }
      case _ => ()
    }

    // write
    val metrics = try {
      writeSparkDataFrameToPath(dfPrepared, hadoopPath, finalSaveMode)
    } catch {
      // cleanup partition directory on failure to ensure restartability for PartitionDiffMode.
      case t: Throwable if partitionValues.nonEmpty && SparkSaveMode.from(finalSaveMode) == SaveMode.Overwrite =>
        deletePartitions(filterPartitionsExisting(partitionValues))
        throw t
    }

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)

    // rename files according to SparkRepartitionDef
    sparkRepartition.foreach(_.renameFiles(getFileRefs(partitionValues))(filesystem))

    // return
    metrics ++ metrics.get("records_written").map("rows_inserted" -> _) // standardize inserted metric
  }

  override private[smartdatalake] def writeSparkDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): MetricsMap = {
    val hadoopPathString = path.toString
    logger.info(s"($id) Writing DataFrame to $hadoopPathString")

    val metrics = SparkStageMetricsListener.execWithMetrics(this.id,
      df.write.format(format)
        .mode(SparkSaveMode.from(finalSaveMode))
        .options(options)
        .optionalPartitionBy(partitions)
        .save(hadoopPathString)
    )

    // recreate current schema file, it gets deleted by SaveMode.Overwrite - this is to avoid schema inference and remember the schema if there is no data.
    if (SparkSaveMode.from(finalSaveMode) == SaveMode.Overwrite) createSchemaFile(df)

    // return
    metrics
  }

  /**
   * Filters only existing partition.
   * Note that partition values to check don't need to have a key/value defined for every partition column.
   */
  def filterPartitionsExisting(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Seq[PartitionValues]  = {
    val partitionValueKeys = PartitionValues.getPartitionValuesKeys(partitionValues).toSeq
    partitionValues.intersect(listPartitions.map(_.filterKeys(partitionValueKeys)))
  }

  /**
   * Compact partitions using Spark
   */
  override def compactPartitions(partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Unit = {
    CompactionUtil.compactHadoopStandardPartitions(this, partitionValues)
  }

  /**
   * Check if this DataObject implements reading data using a Spark V2 DataSource.
   */
  def isV2ReadDataSource(implicit context: ActionPipelineContext): Boolean = {
    DataSource.lookupDataSourceV2(readFormat, context.sparkSession.sessionState.conf).isDefined
  }

}

object SparkFileDataObject extends SmartDataLakeLogger {
  /**
   * This method is searching for files processed by a given DataFrame by looking at its execution plan.
   */
  private[smartdatalake] def getFilesProcessedFromSparkPlan(id: String, df: Dataset[_]): Seq[String] = {
    val fileSources = df.queryExecution.executedPlan.collect { case x: FileSourceScanExec => x }
    if (fileSources.isEmpty) throw new IllegalStateException(s"($id) No FileSourceScanExec found in execution plan to check if there is data to process")
    fileSources.flatMap(_.inputRDD.asInstanceOf[FileScanRDD].filePartitions.flatMap(_.files).map(_.filePath.toString()))
  }
  private[smartdatalake] def tryGetFilesProcessedFromSparkPlan(id: String, df: Dataset[_]): Option[Seq[String]] = try {
    Some(getFilesProcessedFromSparkPlan(id, df))
  } catch {
    case x: IllegalStateException =>
      logger.warn(x.getMessage)
      None
  }
}

/**
 * An interface to implement observing filenames processed.
 */
private[smartdatalake] abstract class SparkFilenameObservation[T](name: String) extends SparkObservation(name) with SmartDataLakeLogger {

  /**
   * Setup observation of custom metric on Dataset.
   */
  def on[T](ds: Dataset[T], filenameColumnName: String): Dataset[T]

  /**
   * Get processed files observation result.
   * Note that this blocks until the query finished successfully. Call only after Spark action was started on observed Dataset.
   */
  def getFilesProcessed: Seq[String]
}


/**
 * Get files processed by using Spark observer on filename column
 * Note: There is a Spark problem - NullPointerException with TypedImperativeAggregate (like CollectSetDeterministic) in observe if there is no data, but sometimes also occurs otherwise on prod...
 * see also https://issues.apache.org/jira/browse/SPARK-39044
 */
private[smartdatalake] class ObserverSparkFilenameObservation[T](name: String) extends SparkFilenameObservation[T](name) {
  def on[T](ds: Dataset[T], filenameColumnName: String): Dataset[T] = {
    logger.debug(s"($name) add files observation to Dataset")
    on(ds, true, collect_set_deterministic(col(filenameColumnName)).as("filesProcessed"))
  }

  def getFilesProcessed: Seq[String] = {
    waitFor().getOrElse("filesProcessed", throw new IllegalStateException(s"($name) Did not receive filesProcessed observation!"))
      .asInstanceOf[Seq[String]]
  }
}

/**
 * Workaround for bug in ObserverSparkFilenameObservation - get files processed from DataFrames execution plan.
 * Note that this might be incorrect if there are additional filters applied.
 */
private[smartdatalake] class ExecutionPlanSparkFilenameObservation[T](name: String) extends SparkFilenameObservation[T](name) {

  private var filesInExecutionPlan: Option[Seq[String]] = None

  def on[T](ds: Dataset[T], filenameColumnName: String): Dataset[T] = {
    logger.debug(s"($name) add files observation to Dataset")
    filesInExecutionPlan = Some(SparkFileDataObject.getFilesProcessedFromSparkPlan(name, ds))
    ds
  }

  override def getFilesProcessed: Seq[String] = {
    val files = filesInExecutionPlan.getOrElse(throw new IllegalStateException(s"($name) filesInExecutionPlan is empty!"))
    if (logger.isDebugEnabled()) logger.debug(s"($name) files processed: ${files.mkString(", ")}")
    files
  }
}