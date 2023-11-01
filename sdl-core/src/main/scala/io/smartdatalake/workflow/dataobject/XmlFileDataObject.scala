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
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.NoDataToProcessWarning
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import io.smartdatalake.workflow.dataframe.{GenericDataFrame, GenericSchema}
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql.functions.{input_file_name, lit}
import org.apache.spark.sql.types.StructType
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

  override val format = "com.databricks.spark.xml"

  // this is only needed for FileRef actions
  override val fileName: String = "*.xml*"

  override val options: Map[String, String] = xmlOptions.getOrElse(Map()) ++ Seq(rowTag.map("rowTag" -> _)).flatten

  override def afterRead(df: DataFrame)(implicit context: ActionPipelineContext): DataFrame  = {
    val dfSuper = super.afterRead(df)
    if (flatten) {
      val parser = new DefaultFlatteningParser()
      parser.parse(dfSuper)
    } else dfSuper
  }

  override def writeSparkDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitions.isEmpty, "writing XML-Files with partitions is not supported by spark-xml")
    val metrics = super.writeSparkDataFrameToPath(df, path, finalSaveMode)
    // add file extension to files, as spark-xml does not out-of-the-box
    filesystem.globStatus(new Path(path, "part-*"), (path: Path) => !path.getName.contains("."))
      .foreach(f => filesystem.rename(f.getPath, f.getPath.suffix(fileName.replace("*", ""))))
    // return
    metrics
  }

  override def factory: FromConfigFactory[DataObject] = XmlFileDataObject
}

object XmlFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): XmlFileDataObject = {
    extract[XmlFileDataObject](config)
  }
}


