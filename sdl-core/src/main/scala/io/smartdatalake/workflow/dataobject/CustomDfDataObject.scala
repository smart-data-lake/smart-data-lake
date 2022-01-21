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
import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.workflow.dataframe.GenericSchema
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.DataFrameUtil
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import io.smartdatalake.workflow.dataframe.spark.SparkSchema
import io.smartdatalake.workflow.{ActionPipelineContext, ExecutionPhase}
import org.apache.spark.sql.DataFrame

/**
 * Generic [[DataObject]] containing a config object.
 * E.g. used to implement a CustomAction that reads a Webservice.
 */
case class CustomDfDataObject(override val id: DataObjectId,
                              creator: CustomDfCreatorConfig,
                              override val schemaMin: Option[GenericSchema] = None,
                              override val metadata: Option[DataObjectMetadata] = None
                             )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with CanCreateSparkDataFrame with SchemaValidation {

  override def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext): DataFrame = {
    implicit val session = context.sparkSession

    // During the init phase, we want to enable getting the schema without creating the entire DataFrame
    // Because during the exec phase, the DataFrame will be created again anyway, leading to multiple calls to creator.exec
    // If the CustomDfDataObject reads lots of data from e.g. a webservice, this might be expensive
    val df = creator.schema match {
      case Some(schema) if context.phase != ExecutionPhase.Exec => DataFrameUtil.getEmptyDataFrame(schema)
      case _ => creator.exec
    }

    validateSchemaMin(SparkSchema(df.schema), "read")
    df
  }

  override def factory: FromConfigFactory[DataObject] = CustomDfDataObject
}

object CustomDfDataObject extends FromConfigFactory[DataObject] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): CustomDfDataObject = {
    extract[CustomDfDataObject](config)
  }
}

