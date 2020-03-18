/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow.action.customlogic.CustomDfCreatorConfig
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Generic [[DataObject]] containing a config object.
 * E.g. used to implement a CustomAction that reads a Webservice.
 */
case class CustomDfDataObject(override val id: DataObjectId,
                              creator: CustomDfCreatorConfig,
                              override val schemaMin: Option[StructType] = None,
                              override val metadata: Option[DataObjectMetadata] = None
                             )(@transient implicit val instanceRegistry: InstanceRegistry)
  extends DataObject with CanCreateDataFrame with SchemaValidation {

  override def getDataFrame(implicit session: SparkSession) : DataFrame = {
    val df = creator.exec
    validateSchemaMin(df)
    df
  }

  /**
   * @inheritdoc
   */
  override def factory: FromConfigFactory[DataObject] = CustomDfDataObject
}

object CustomDfDataObject extends FromConfigFactory[DataObject] {

  /**
   * @inheritdoc
   */
  override def fromConfig(config: Config, instanceRegistry: InstanceRegistry): CustomDfDataObject = {
    import configs.syntax.ConfigOps
    import io.smartdatalake.config._

    implicit val instanceRegistryImpl: InstanceRegistry = instanceRegistry
    config.extract[CustomDfDataObject].value
  }
}

