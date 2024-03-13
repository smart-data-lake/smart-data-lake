/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2023 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Returns dataframe with only unique columns based on a primary key
 * @param name              Name of the transformer
 * @param description       Optional description of the transformer
 * @param rankingExpression Ranking expression to determine duplicates
 * @param primaryKeyColumns Optional list of primary key columns
 */
case class DeduplicateTransformer(override val name: String = "DeduplicateTransformer", override val description: Option[String] = None, val rankingExpression: String, val primaryKeyColumns: Option[Seq[String]] = None) extends GenericDfTransformer {

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {


    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._



    df
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = ConvertNullValuesTransformer
}

object DeduplicateTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeduplicateTransformer = {
    extract[DeduplicateTransformer](config)
  }
}

