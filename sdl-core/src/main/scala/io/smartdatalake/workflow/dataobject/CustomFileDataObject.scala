/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 Schweizerische Bundesbahnen SBB (<https://www.sbb.ch>)
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

import java.io.InputStream
import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SDLSaveMode.SDLSaveMode
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.customlogic.CustomFileCreatorConfig
import org.apache.spark.sql.{SaveMode, SparkSession}

case class CustomFileDataObject(override val id: DataObjectId,
                                creator: CustomFileCreatorConfig,
                                override val metadata: Option[DataObjectMetadata] = None
                               )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with FileRefDataObject with CanCreateInputStream {

  override def createInputStream(path: String)(implicit session: SparkSession): InputStream = {
    creator.exec
  }

  override def partitionLayout(): Option[String] = None

  override def getFileRefs(partitionValues: Seq[PartitionValues])(implicit session: SparkSession): Seq[FileRef] = {
    Seq(FileRef("custom", "custom", PartitionValues(Map())))
  }

  override def saveMode: SDLSaveMode =
    throw new ConfigurationException("CustomFileDataObject does not support being written to")

  override def path: String =
    throw new ConfigurationException("CustomFileDataObject does not support being written to")

  override def partitions: Seq[String] = Seq()

  override def expectedPartitionsCondition: Option[String] = None

  override def listPartitions(implicit session: SparkSession, context: ActionPipelineContext): Seq[PartitionValues] = Seq()

  override def relativizePath(filePath: String)(implicit session: SparkSession): String = filePath

  override def factory: FromConfigFactory[DataObject] = CustomFileDataObject
}

object CustomFileDataObject extends FromConfigFactory[DataObject] with SmartDataLakeLogger {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomFileDataObject = {
    extract[CustomFileDataObject](config)
  }
}
