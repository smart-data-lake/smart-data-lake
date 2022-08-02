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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.json.DefaultFlatteningParser
import io.smartdatalake.util.misc.AclDef
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.util.spark.DataFrameUtil.DataFrameReaderUtils
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{input_file_name, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A [[DataObject]] backed by an XML data source.
 *
 * It manages read and write access and configurations required for [[Action]]s to
 * work on XML formatted files.
 *
 * Reading and writing details are delegated to Apache Spark [[org.apache.spark.sql.DataFrameReader]]
 * and [[org.apache.spark.sql.DataFrameWriter]] respectively. The reader and writer implementations are provided by
 * the [[https://github.com/databricks/spark-xml databricks spark-xml]] project.
 * Note that writing XML-file partitioned is not supported by spark-xml.
 *
 * @param xmlOptions Settings for the underlying [[org.apache.spark.sql.DataFrameReader]] and [[org.apache.spark.sql.DataFrameWriter]].
 * @param schema An optional data object schema. If defined, any automatic schema inference is avoided.
 *               As this corresponds to the schema on write, it must not include the optional filenameColumn on read.
 *               Define the schema by using one of the schema providers DDL, jsonSchemaFile, xsdFile or caseClassName.
 *               The schema provider and its configuration value must be provided in the format <PROVIDERID>#<VALUE>.
 *               A DDL-formatted string is a comma separated list of field definitions, e.g., a INT, b STRING.
 * @param sparkRepartition Optional definition of repartition operation before writing DataFrame with Spark to Hadoop.
 * @param expectedPartitionsCondition Optional definition of partitions expected to exist.
 *                                    Define a Spark SQL expression that is evaluated against a [[PartitionValues]] instance and returns true or false
 *                                    Default is to expect all partitions to exist.
 * @param housekeepingMode Optional definition of a housekeeping mode applied after every write. E.g. it can be used to cleanup, archive and compact partitions.
 *                         See HousekeepingMode for available implementations. Default is None.
 *
 * @see [[org.apache.spark.sql.DataFrameReader]]
 * @see [[org.apache.spark.sql.DataFrameWriter]]
 */
case class XmlFileDataObject(override val id: DataObjectId,
                             override val path: String,
                             rowTag: Option[String] = None, // this is for backward compatibility, it can also be given in xmlOptions
                             xmlOptions: Option[Map[String,String]] = None,
                             override val partitions: Seq[String] = Seq(),
                             override val schema: Option[GenericSchema] = None,
                             override val schemaMin: Option[GenericSchema] = None,
                             override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                             override val sparkRepartition: Option[SparkRepartitionDef] = None,
                             flatten: Boolean = false,
                             override val acl: Option[AclDef] = None,
                             override val connectionId: Option[ConnectionId] = None,
                             override val filenameColumn: Option[String] = None,
                             override val expectedPartitionsCondition: Option[String] = None,
                             override val housekeepingMode: Option[HousekeepingMode] = None,
                             override val metadata: Option[DataObjectMetadata] = None)
                            (@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject {

  override val format = "com.databricks.spark.xml.patched"

  // this is only needed for FileRef actions
  override val fileName: String = "*.xml*"

  override val options: Map[String, String] = xmlOptions.getOrElse(Map()) ++ Seq(rowTag.map("rowTag" -> _)).flatten

  /**
   * Constructs an Apache Spark [[DataFrame]] from the underlying file content.
   *
   * As spark-xml doesn't support reading partitions, SDL needs to handle partitions on its own.
   * This method overwrites standard getDataFrame method of SparkFileDataObject for this purpose.
   *
   * Example for spark-xml failure: reading partitioned XML-files with results in FileNotFoundException
   *   session.read
   *   .format("xml")
   *   .options(Map("rowTag" -> "report"))
   *   .schema(rawData.schema.get)
   *   .load("partitionedDataObjectPath")
   *   .show
   */
  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session: SparkSession = context.sparkSession
    import io.smartdatalake.util.spark.DataFrameUtil.DfSDL

    val wrongPartitionValues = PartitionValues.checkWrongPartitionValues(partitionValues, partitions)
    assert(wrongPartitionValues.isEmpty, s"getDataFrame got request with PartitionValues keys ${wrongPartitionValues.mkString(",")} not included in $id partition columns ${partitions.mkString(", ")}")
    assert(partitionValues.forall(pv => partitions.toSet.diff(pv.keys).isEmpty), "PartitionValues must include values for all partitions when reading XML-Data")

    val schemaOpt = getSchema.map(_.inner)
    if (!(schemaOpt.isDefined || checkFilesExisting)) {
      //without either schema or data, no data frame can be created
      require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined if there are no existing files.")
    }

    // Hadoop directory must exist for creating DataFrame below. Reading the DataFrame on read also for not yet existing data objects is needed to build the spark lineage of DataFrames.
    if (!filesystem.exists(hadoopPath)) filesystem.mkdirs(hadoopPath)

    val dfContent = if (partitions.isEmpty) {
      session.read
        .format(format)
        .options(options)
        .optionalSchema(schemaOpt)
        .option("path", hadoopPath.toString) // spark-xml is a V1 source and only supports one path, which must be given as option...
        .load()
    } else {
      val reader = session.read
        .format(format)
        .options(options)
        .optionalSchema(schemaOpt)
      val partitionValuesToRead = if (partitionValues.nonEmpty) partitionValues else listPartitions
      // create data frame for every partition value and then build union
      val pathsToRead = partitionValuesToRead.flatMap(pv => getConcretePaths(pv).map(path => (pv, path.toString)))
      if (pathsToRead.nonEmpty)
        pathsToRead.map { case (pv, path) =>
          partitions.foldLeft(reader.option("path", path).load()) {
            case (df,partition) => df.withColumn(partition, lit(pv(partition).toString))
          }
        }.reduce(_ union _)
      else {
        // if there are no paths to read then an empty DataFrame is created
        require(schema.isDefined, s"($id) DataObject schema is undefined. A schema must be defined as there are no existing files for partition values ${partitionValues.mkString(", ")}.")
        DataFrameUtil.getEmptyDataFrame(schemaOpt.get)
      }
    }

    val df = dfContent.withOptionalColumn(filenameColumn, input_file_name)

    // finalize & return DataFrame
    afterRead(df)
  }

  override def afterRead(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame  = {
    val dfSuper = super.afterRead(df)
    if (flatten) {
      val parser = new DefaultFlatteningParser()
      parser.parse(dfSuper)
    } else dfSuper
  }

  override def writeDataFrameToPath(df: GenericDataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): Unit = {
    assert(partitions.isEmpty, "writing XML-Files with partitions is not supported by spark-xml")
    super.writeDataFrameToPath(df, path, finalSaveMode)
  }

  override def factory: FromConfigFactory[DataObject] = XmlFileDataObject
}

object XmlFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): XmlFileDataObject = {
    extract[XmlFileDataObject](config)
  }
}


