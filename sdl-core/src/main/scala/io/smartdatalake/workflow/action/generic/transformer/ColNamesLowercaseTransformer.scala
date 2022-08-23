/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2022 ELCA Informatique SA (<https://www.elca.ch>)
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

package io.smartdatalake.workflow.action.generic.transformer

import com.typesafe.config.Config
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}


/**
 * Convert all column names to lowercase.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param camelCaseToLower if selected, converts Camel case names to lower case  with underscores, i.e. TestString -> test_string
 */
case class ColNamesLowercaseTransformer(override val name: String = "colNamesLowercase", override val description: Option[String] = None, camelCaseToLower: Boolean=false) extends GenericDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    implicit val functions: DataFrameFunctions = DataFrameSubFeed.getFunctions(df.subFeedType)
    if (camelCaseToLower) {
      df.colCamelNamesLowercase
    } else {
      df.colNamesLowercase
    }
  }
  override def factory: FromConfigFactory[GenericDfTransformer] = ColNamesLowercaseTransformer
}

object ColNamesLowercaseTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): ColNamesLowercaseTransformer = {
    extract[ColNamesLowercaseTransformer](config)
  }
}