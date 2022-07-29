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
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.{CompactionUtil, EnvironmentUtil, SmartDataLakeLogger}
import io.smartdatalake.util.spark.CollectSetDeterministic.collect_set_deterministic
import io.smartdatalake.util.spark.DataFrameUtil.{DataFrameReaderUtils, DataFrameWriterUtils}
import io.smartdatalake.util.spark.{DataFrameUtil, Observation}
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ExecutionPhase, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.FileScanRDD
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.types.{DataType, StructType}

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
private[smartdatalake] trait SparkFileDataObject extends HadoopFileDataObject
  with CanCreateSparkDataFrame with CanCreateStreamingDataFrame
  with CanWriteSparkDataFrame with CanCreateIncrementalOutput
  with UserDefinedSchema with SchemaValidation {

  /**
   * The Spark-Format provider to be used
   */
  def format: String

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
    schema.foreach(schemaExpected => validateSchema(SparkSchema(df.schema), schemaExpected, "write"))
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
    schema.map(createReadSchema).foreach(schemaExpected => validateSchema(SparkSchema(df.schema), schemaExpected, "read"))
    df
  }

  /**
   * Returns the user-defined schema for reading from the data source. By default, this should return `schema` but it
   * may be customized by data objects that have a source schema and ignore the user-defined schema on read operations.
   *
   * If a user-defined schema is returned, it overrides any schema inference. If no user-defined schema is set, the
   * schema may be inferred depending on the configuration and type of data frame reader.
   *
   * @return The schema to use for the data frame reader when reading from the source.
   */
  def getSchema(implicit context: ActionPipelineContext): Option[SparkSchema] = {
    import io.smartdatalake.util.spark.DataFrameUtil.DfSDL
    _schemaHolder = _schemaHolder.orElse(
        // get defined schema
        schema.map(_.convert(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema])
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
          val df = context.sparkSession.read
            .format(format)
            .options(options)
            .load(sampleFile.toString)
            .withOptionalColumn(filenameColumn, input_file_name)
          val dfWithPartitions = partitions.foldLeft(df) {
            case (df, p) => df.withColumn(p, functions.lit("dummyString"))
          }
          Some(SparkSchema(dfWithPartitions.schema))
        } else None
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

  /**
   * Provide a sample data file name to be created to file-based Action. If none is returned, no file is created.
   */
  override def createSampleFile(implicit context: ActionPipelineContext): Option[String] = {
    // only create new sample file there is no schema file and if it doesnt exist yet, or an update is forced by the environment configuration
    if (!filesystem.exists(schemaFile) && (Environment.updateSparkFileDataObjectSampleDataFile || !filesystem.exists(sampleFile))) Some(sampleFile.toString)
    else None
  }
  private def sampleFile(implicit context: ActionPipelineContext) = new Path( new Path(hadoopPath, ".sample"), s"sampleData.${fileName.split('.').last.filter(_ != '*')}")

  override def options: Map[String, String] = Map() // override options because of conflicting definitions in CanCreateSparkDataFrame and CanWriteSparkDataFrame

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

    val incrementalOutputOptions = if (incrementalOutputState.isDefined && context.phase == ExecutionPhase.Exec) {
      val previousOutputState = incrementalOutputState.get
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
    } else Map[String,String]()

    val dfContent = if (partitions.isEmpty || partitionValues.isEmpty) {
      session.read
        .format(format)
        .options(options ++ incrementalOutputOptions)
        .optionalSchema(schemaOpt)
        .load(hadoopPath.toString)
    } else {
      val reader = session.read
        .format(format)
        .options(options ++ incrementalOutputOptions)
        .optionalSchema(schemaOpt)
        .option("basePath", hadoopPath.toString) // this is needed for partitioned tables when subdirectories are read directly; it then keeps the partition columns from the subdirectory path in the dataframe
      val pathsToRead = partitionValues.flatMap(getConcretePaths).map(_.toString)
      val df = if (pathsToRead.nonEmpty) Some(reader.load(pathsToRead:_*)) else None
      df.filter(df => schemaOpt.isDefined || partitions.diff(df.columns).isEmpty) // filter DataFrames without partition columns as they are empty (this might happen if there is no schema specified and the partition is empty)
        .getOrElse {
          // if there are no paths to read then an empty DataFrame is created
          require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined as there are no existing files for partition values ${partitionValues.mkString(", ")}.")
          DataFrameUtil.getEmptyDataFrame(schemaOpt.get)
        }
    }

    // early check for no data to process.
    // This also prevents an error on Databricks when using filesObserver if there are no files to process. The
    if (context.phase == ExecutionPhase.Exec && getFilesProcessedFromSparkPlan(dfContent).isEmpty)
      throw NoDataToProcessWarning("-", s"($id) No files to process found in execution plan")

    // add filename column
    var df = dfContent.withOptionalColumn(filenameColumn, input_file_name)

    // configure observer to get files processed for incremental execution mode
    if (filesObservers.nonEmpty && context.phase == ExecutionPhase.Exec) {
      if (filesObservers.size > 1) logger.warn(s"($id) files observation is not yet well supported when using from multiple actions in parallel")
      // force creating filenameColumn, and drop the it later again
      val forcedFilenameColumn = "__filename"
      if (filenameColumn.isEmpty) df = dfContent.withColumn(forcedFilenameColumn, input_file_name)
      // initialize observers
      df = filesObservers.foldLeft(df) {
        case (df, (actionId,observer)) => observer.on(df, filenameColumn.getOrElse(forcedFilenameColumn))
      }
      filesObservers.clear
      // drop forced filenameColumn
      if (filenameColumn.isEmpty) df = df.drop(forcedFilenameColumn)
    }

    // finalize & return DataFrame
    afterRead(df)
  }

  /**
   * This method is searching for files processed by a given DataFrame by looking at its execution plan.
   */
  private[smartdatalake] def getFilesProcessedFromSparkPlan(df: DataFrame): Seq[String] = {
    df.queryExecution.executedPlan.collectFirst { case x: FileSourceScanExec => x }
      .getOrElse(throw new IllegalStateException(s"($id) No FileSourceScanExec found in execution plan to check if there is data to process"))
      .inputRDD.asInstanceOf[FileScanRDD].filePartitions.flatMap(_.files).map(_.filePath)
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
  private val filesObservers: mutable.Map[ActionId, FilesObservation] = mutable.Map()

  /**
   * Setup an observation of files processed through custom metrics.
   * This is used for incremental processing to keep track of files processed.
   * Note that filenameColumn needs to be configured for the DataObject in order for this to work.
   */
  def setupFilesObserver(actionId: ActionId): FilesObservation = {
    logger.debug(s"($id) setting up files observer for $actionId")
    // return existing observation for this action if existing, otherwise create a new one.
    filesObservers.getOrElseUpdate(actionId, new FilesObservation(actionId.id + "/" + id.id))
  }

  override def getStreamingDataFrame(options: Map[String, String], pipelineSchema: Option[StructType])(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    require(schema.orElse(pipelineSchema).isDefined, s"($id}) Schema must be defined for streaming SparkFileDataObject")
    // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
    if (!filesystem.exists(hadoopPath.getParent)) filesystem.mkdirs(hadoopPath)

    val schemaOpt = getSchema.map(_.inner).orElse(pipelineSchema).get
    val df = session.readStream
      .format(format)
      .options(options ++ this.options)
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
    createSchemaFile(df)
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
                             (implicit context: ActionPipelineContext): Unit = {
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
      case _ => Unit
    }

    // write
    try {
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
  }

  override private[smartdatalake] def writeSparkDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): Unit = {
    val hadoopPathString = path.toString
    logger.info(s"($id) Writing DataFrame to $hadoopPathString")

    df.write.format(format)
      .mode(SparkSaveMode.from(finalSaveMode))
      .options(options)
      .optionalPartitionBy(partitions)
      .save(hadoopPathString)

    // recreate current schema file, it gets deleted by SaveMode.Overwrite - this is to avoid schema inference and remember the schema if there is no data.
    if (SparkSaveMode.from(finalSaveMode) == SaveMode.Overwrite) createSchemaFile(df)
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

}

/**
 * Observation of files processed using custom metrics.
 */
private[smartdatalake] class FilesObservation(name: String) extends Observation(name) with SmartDataLakeLogger {

  /**
   * Setup observation of custom metric on Dataset.
   */
  def on[T](ds: Dataset[T], filenameColumnName: String): Dataset[T] = {
    logger.debug(s"($name) add files observation to Dataset")
    on(ds, collect_set_deterministic(col(filenameColumnName)).as("filesProcessed"))
  }

  /**
   * Get processed files observation result.
   * Note that this blocks until the query finished successfully. Call only after Spark action was started on observed Dataset.
   */
  def getFilesProcessed: Seq[String] = {
    val files = waitFor().getOrElse("filesProcessed", throw new IllegalStateException(s"($name) Did not receive filesProcessed observation!"))
      .asInstanceOf[Seq[String]]
    if (logger.isDebugEnabled()) logger.debug(s"($name) files processed: ${files.mkString(", ")}")
    files
  }
}