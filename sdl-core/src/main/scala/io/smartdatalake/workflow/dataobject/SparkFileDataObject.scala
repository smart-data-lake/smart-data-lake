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

import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.{HdfsUtil, PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.DataFrameUtil.{DataFrameReaderUtils, DataFrameWriterUtils}
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.input_file_name

/**
 * A [[DataObject]] backed by a file in HDFS. Can load file contents into an Apache Spark [[DataFrame]]s.
 *
 * Delegates read and write operations to Apache Spark [[DataFrameReader]] and [[DataFrameWriter]] respectively.
 */
private[smartdatalake] trait SparkFileDataObject extends HadoopFileDataObject with CanCreateDataFrame with CanWriteDataFrame with CanCreateStreamingDataFrame
  with UserDefinedSchema with SchemaValidation {

  /**
   * The Spark-Format provider to be used
   */
  def format: String

  /**
   * Returns the configured options for the Spark [[DataFrameReader]]/[[DataFrameWriter]].
   *
   * @see [[DataFrameReader]]
   * @see [[DataFrameWriter]]
   */
  def options: Map[String, String] = Map()

  /**
   * The name of the (optional) additional column containing the source filename
   */
  def filenameColumn: Option[String]

  /**
   * Definition of repartition operation before writing DataFrame with Spark to Hadoop.
   */
  def sparkRepartition: Option[SparkRepartitionDef]
  assert(sparkRepartition.flatMap(_.filename).isEmpty || partitions.isEmpty, s"($id) Cannot rename file with SparkRepartitionDef for partitioned DataObject")

  /**
   * Callback that enables potential transformation to be applied to `df` before the data is written.
   *
   * Default is to validate the `schemaMin` and not apply any modification.
   */
  def beforeWrite(df: DataFrame): DataFrame = {
    validateSchemaMin(df)
    df
  }

  /**
   * Callback that enables potential transformation to be applied to `df` after the data is read.
   *
   * Default is to validate the `schemaMin` and not apply any modification.
   */
  def afterRead(df: DataFrame): DataFrame = {
    validateSchemaMin(df)
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
  def readSchema(sourceExists: Boolean): Option[StructType] = schema

  /**
   * Constructs an Apache Spark [[DataFrame]] from the underlying file content.
   *
   * @see [[DataFrameReader]]
   *
   * @param session the current [[SparkSession]].
   * @return a new [[DataFrame]] containing the data stored in the file at `path`
   */
  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit session: SparkSession, context: ActionPipelineContext) : DataFrame = {
    import io.smartdatalake.util.misc.DataFrameUtil.DfSDL

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

    val dfContent = if (partitions.isEmpty || partitionValues.isEmpty) {
      session.read
        .format(format)
        .options(options)
        .optionalSchema(readSchema(filesExists))
        .load(hadoopPath.toString)
    } else {
      val reader = session.read
        .format(format)
        .options(options)
        .optionalSchema(readSchema(filesExists))
        .option("basePath", hadoopPath.toString) // this is needed for partitioned tables when subdirectories are read directly; it then keeps the partition columns from the subdirectory path in the dataframe
      // create data frame for every partition value and then build union
      val pathsToRead = partitionValues.flatMap(getConcretePaths).map(_.toString)
      pathsToRead.map(reader.load).reduce(_ union _)
    }

    val df = dfContent.withOptionalColumn(filenameColumn, input_file_name)

    // finalize & return DataFrame
    afterRead(df)
  }

  override def getStreamingDataFrame(options: Map[String,String], pipelineSchema: Option[StructType])(implicit session: SparkSession): DataFrame = {
    require(schema.orElse(pipelineSchema).isDefined, s"(${id}) Schema must be defined for streaming SparkFileDataObject")
    // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
    if (!filesystem.exists(hadoopPath.getParent)) filesystem.mkdirs(hadoopPath)

    val df = session.readStream
      .format(format)
      .options(options)
      .schema(schema.orElse(pipelineSchema).get)
      .load(hadoopPath.toString)

    afterRead(df)
  }

  override def createReadSchema(writeSchema: StructType)(implicit session: SparkSession): StructType = {
    // add additional columns created by SparkFileDataObject
    filenameColumn.map(colName => addFieldIfNotExisting(writeSchema, colName, StringType))
      .getOrElse(writeSchema)
  }

  /**
   * Writes the provided [[DataFrame]] to the filesystem.
   *
   * The `partitionValues` attribute is used to partition the output by the given columns on the file system.
   *
   * @see [[DataFrameWriter.partitionBy]]
   * @param df the [[DataFrame]] to write to the file system.
   * @param partitionValues The partition layout to write.
   * @param session the current [[SparkSession]].
   */
  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false)(implicit session: SparkSession): Unit = {
    require(!isRecursiveInput, "($id) SparkFileDataObject cannot write dataframe when dataobject is also used as recursive input ")

    // prepare data
    var dfPrepared = beforeWrite(df)
    dfPrepared = sparkRepartition.map(_.prepareDataFrame(dfPrepared,partitions, partitionValues.size, id))
      .getOrElse(dfPrepared)

    // delete concerned partitions if existing
    if (saveMode == SaveMode.Overwrite && partitions.nonEmpty) {
      val partitionValuesCols = partitionValues.map(_.keys).reduceOption(_ ++ _).getOrElse(Set()).toSeq
      val partitionValuesToDelete = partitionValues.intersect(listPartitions.map(_.filterKeys(partitionValuesCols)))
      deletePartitions(partitionValuesToDelete)
    }

    // Workaround ADLSv2: overwrite unpartitioned data object
    if (saveMode == SaveMode.Overwrite && partitions.isEmpty && Environment.enableOverwriteUnpartitionedSparkFileDataObjectAdls) {
      deleteAll
    }

    val hadoopPathString = hadoopPath.toString
    logger.info(s"Writing data frame to $hadoopPathString")

    // write
    dfPrepared.write.format(format)
      .mode(saveMode)
      .options(options)
      .optionalPartitionBy(partitions)
      .save(hadoopPathString)

    // make sure empty partitions are created as well
    createMissingPartitions(partitionValues)

    // rename file according to sparkRepartition (only if not partitioned and numberOfTasksPerPartition=1)
    sparkRepartition.flatMap(_.filename).foreach {
      filename =>
        require(partitions.isEmpty, s"($id) Cannot rename file with SparkRepartitionDef for partitioned DataObject")
        val files = getFileRefs(Seq())
        require(files.size <= 1, s"($id) Number of files should not be greater than 1 because SparkRepartitionDef.numberOfTasksPerPartition should be set to 1 if filename is set!")
        files.map(f => new Path(f.fullPath)).foreach( p => filesystem.rename(p, new Path(p.getParent, filename)))
    }
  }
}

/**
 * A [[DataObject]] backed by a file in HDFS with an embedded schema.
 */
private[smartdatalake] trait SparkFileDataObjectWithEmbeddedSchema extends SparkFileDataObject {
    override def readSchema(filesExist: Boolean): Option[StructType] = {
        // If the source exists, it has an embedded schema. In this case, ignore the user defined schema.
        if (filesExist && schema.isDefined) {
            logger.warn(s"($id) User-defined schema is configured but ignored because the source file contains a schema.")
            None
        } else {
            schema
        }
    }
}
