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

import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.definitions.{Environment, SDLSaveMode, SaveModeOptions}
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.spark.DataFrameUtil.{DataFrameReaderUtils, DataFrameWriterUtils}
import io.smartdatalake.util.misc.CompactionUtil
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.dataframe.spark.{SparkSchema, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed, ProcessingLogicException}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructType

import scala.reflect.runtime.universe.typeOf

/**
 * A [[DataObject]] backed by a file in HDFS. Can load file contents into an Apache Spark [[DataFrame]]s.
 *
 * Delegates read and write operations to Apache Spark [[DataFrameReader]] and [[DataFrameWriter]] respectively.
 */
private[smartdatalake] trait SparkFileDataObject extends HadoopFileDataObject
  with CanCreateDataFrame with CanCreateSparkDataFrame with CanCreateStreamingDataFrame
  with CanWriteDataFrame with CanWriteSparkDataFrame
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
   * @param sourceExists Whether the source file/table exists already. Existing sources may have a source schema.
   * @return The schema to use for the data frame reader when reading from the source.
   */
  def getSchema(sourceExists: Boolean): Option[SparkSchema] = schema.map(_.convertIfNeeded(typeOf[SparkSubFeed]).asInstanceOf[SparkSchema])

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

    val filesExists = checkFilesExisting
    if (!filesExists) {
      //without either schema or data, no data frame can be created
      require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined if there are no existing files.")

      // Schema exists so an empty data frame can be created
      // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
      filesystem.mkdirs(hadoopPath)
    }

    val schemaOpt = getSchema(filesExists).map(_.inner)
    val dfContent = if (partitions.isEmpty || partitionValues.isEmpty) {
      session.read
        .format(format)
        .options(options)
        .optionalSchema(schemaOpt)
        .load(hadoopPath.toString)
    } else {
      val reader = session.read
        .format(format)
        .options(options)
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

    val df = dfContent.withOptionalColumn(filenameColumn, input_file_name)

    // finalize & return DataFrame
    afterRead(df)
  }

  override def getStreamingDataFrame(options: Map[String, String], pipelineSchema: Option[StructType])(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    require(schema.orElse(pipelineSchema).isDefined, s"($id}) Schema must be defined for streaming SparkFileDataObject")
    // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
    if (!filesystem.exists(hadoopPath.getParent)) filesystem.mkdirs(hadoopPath)

    val schemaOpt = getSchema(checkFilesExisting).map(_.inner).orElse(pipelineSchema).get
    val df = session.readStream
      .format(format)
      .options(options ++ this.options)
      .schema(schemaOpt)
      .load(hadoopPath.toString)

    afterRead(df)
  }

  override def createReadSchema(writeSchema: GenericSchema)(implicit context: ActionPipelineContext): GenericSchema = {
    val helper = DataFrameSubFeed.getHelper(writeSchema.subFeedType)
    // add additional columns created by SparkFileDataObject
    filenameColumn.map(colName => addFieldIfNotExisting(writeSchema, colName, helper.stringType))
      .getOrElse(writeSchema)
  }

  override def initSparkDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues], saveModeOptions: Option[SaveModeOptions] = None)(implicit context: ActionPipelineContext): Unit = {
    validateSchemaMin(SparkSchema(df.schema), "write")
    schema.foreach(schemaExpected => validateSchema(SparkSchema(df.schema), schemaExpected, "write"))
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
    writeSparkDataFrameToPath(dfPrepared, hadoopPath, finalSaveMode)

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)

    // rename file according to SparkRepartitionDef
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
