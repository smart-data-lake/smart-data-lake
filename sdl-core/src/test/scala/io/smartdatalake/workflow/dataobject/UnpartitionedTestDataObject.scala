/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2021 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.SaveModeOptions
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * As all of the prepackaged DataObjects implementing CanWriteDataFrame also implement CanHandlePartitions, we need to
 * create a custom DataObject implementation for tests without implementing CanHandlePartitions.
 */
case class UnpartitionedTestDataObject(override val id: DataObjectId,
                                       override val metadata: Option[DataObjectMetadata] = None)
                                      (@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanWriteDataFrame {

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    val session = context.sparkSession
    import session.implicits._
    Seq(("test",1),("test",2)).toDF("a","b")
  }

  override def writeDataFrame(df: DataFrame, partitionValues: Seq[PartitionValues] = Seq(), isRecursiveInput: Boolean = false, saveModeOptions: Option[SaveModeOptions] = None)
                             (implicit context: ActionPipelineContext): Unit = {
    df.show
  }

  override def factory: FromConfigFactory[DataObject] = UnpartitionedTestDataObject
}

object UnpartitionedTestDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): UnpartitionedTestDataObject = {
    extract[UnpartitionedTestDataObject](config)
  }
}

