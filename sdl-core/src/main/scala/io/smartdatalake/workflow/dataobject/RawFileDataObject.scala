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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.{PartitionValues, SparkRepartitionDef}
import io.smartdatalake.util.misc.AclDef
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.workflow.dataframe.spark.SparkSchema

/**
 * DataObject of type raw for files with unknown content.
 * Provides details to an Action to access raw files.
 *
 * By specifying format binary or text files can used with Spark.
 *
 * @param customFormat Custom Spark data source format, e.g. binaryFile or text.
 *                     Only needed if you want to read/write this DataObject with Spark.
 * @param options Options for custom Spark data source format.
 *                Only of use if you want to read/write this DataObject with Spark.
 */
case class RawFileDataObject( override val id: DataObjectId,
                              override val path: String,
                              customFormat: Option[String] = None,
                              override val options: Map[String, String] = Map(),
                              override val fileName: String = "*",
                              override val partitions: Seq[String] = Seq(),
                              override val schema: Option[GenericSchema] = None,
                              override val schemaMin: Option[GenericSchema] = None,
                              override val saveMode: SDLSaveMode = SDLSaveMode.Overwrite,
                              override val sparkRepartition: Option[SparkRepartitionDef] = None,
                              override val acl: Option[AclDef] = None,
                              override val connectionId: Option[ConnectionId] = None,
                              override val filenameColumn: Option[String] = None,
                              override val expectedPartitionsCondition: Option[String] = None,
                              override val housekeepingMode: Option[HousekeepingMode] = None,
                              override val metadata: Option[DataObjectMetadata] = None
                            )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends SparkFileDataObject {

  /** override schema for text and binaryfile format, as these are fixed */
  override def getSchema(implicit context: ActionPipelineContext): Option[SparkSchema] = {
    val isFormatWithFixedSchema = Seq("text", "binaryfile").contains(readFormat.toLowerCase)
    if (isFormatWithFixedSchema && schema.isDefined) throw ConfigurationException(s"($id) Schema is fixed for format=$readFormat. Remove schema attribute from DataObject configuration.")
    if (isFormatWithFixedSchema) Some(inferSchemaFromPath(hadoopPath.toString))
    else super.getSchema
  }

  override def format: String = customFormat.getOrElse(throw ConfigurationException(s"($id) set attribute customFormat if you want to read/write this a RawFileDataObject with Spark"))

  override def factory: FromConfigFactory[DataObject] = RawFileDataObject

}

object RawFileDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): RawFileDataObject = {
    extract[RawFileDataObject](config)
  }
}

