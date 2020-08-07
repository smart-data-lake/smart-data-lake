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
import io.smartdatalake.util.misc.AclDef
import org.apache.spark.sql.SaveMode

/**
 * DataObject of type raw for files with unknown content.
 * Provides details to an Action to access raw files.
 * @param fileName Definition of fileName. This is concatenated with path and partition layout to search for files. Default is an asterix to match everything.
 * @param saveMode Overwrite or Append new data.
 *
 */
case class RawFileDataObject( override val id: DataObjectId,
                              override val path: String,
                              override val fileName: String = "*",
                              override val partitions: Seq[String] = Seq(),
                              override val saveMode: SaveMode = SaveMode.Overwrite,
                              override val acl: Option[AclDef] = None,
                              override val connectionId: Option[ConnectionId] = None,
                              override val metadata: Option[DataObjectMetadata] = None
                            )(@transient implicit override val instanceRegistry: InstanceRegistry)
  extends HadoopFileDataObject {

  // reading binary files as DataFrame is supported starting from Spark 3.0, see https://docs.databricks.com/data/data-sources/binary-file.html

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = RawFileDataObject

}

object RawFileDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): RawFileDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[RawFileDataObject].value
  }
}

