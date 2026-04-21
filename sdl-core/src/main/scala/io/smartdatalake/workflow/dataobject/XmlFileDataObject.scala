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
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.{SDLSaveMode, SaveModeOptions}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{SparkRepartitionDef, WoodstoxXMLOutputFactory}
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataobject.expectation.Expectation
import io.smartdatalake.workflow.dataobject.generic.{Constraint, HousekeepingMode}
import io.smartdatalake.workflow.dataobject.spark.SparkFileDataObject
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

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
 */
case class XmlFileDataObject(override val id: DataObjectId,
                             override val path: String,
                             xmlOptions: Option[Map[String,String]] = None,
                             override val partitions: Seq[String] = Seq(),
                             override val schema: Option[GenericSchema] = None,
                             override val schemaMin: Option[GenericSchema] = None,
                             override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                             override val sparkRepartition: Option[SparkRepartitionDef] = None,
                             override val connectionId: Option[ConnectionId] = None,
                             override val filenameColumn: Option[String] = None,
                             override val expectedPartitionsCondition: Option[String] = None,
                             override val housekeepingMode: Option[HousekeepingMode] = None,
                             override val constraints: Seq[Constraint] = Seq(),
                             override val expectations: Seq[Expectation] = Seq(),
                             override val metadata: Option[DataObjectMetadata] = None)
                            (@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject {

  override val format = "com.databricks.spark.xml"

  // this is only needed for FileRef actions
  override val fileName: String = "*.xml*"

  override val options: Map[String, String] = Map("pathGlobFilter" -> fileName) ++ xmlOptions.getOrElse(Map())

  override def writeSparkDataFrameToPath(df: DataFrame, path: Path, finalSaveMode: SDLSaveMode)(implicit context: ActionPipelineContext): MetricsMap = {
    assert(partitions.isEmpty, "writing XML-Files with partitions is not supported by spark-xml")
    // Needed in Spark 4.1, see WoodstoxXMLOutputFactory for details
    System.setProperty("javax.xml.stream.XMLOutputFactory", classOf[WoodstoxXMLOutputFactory].getName)
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


