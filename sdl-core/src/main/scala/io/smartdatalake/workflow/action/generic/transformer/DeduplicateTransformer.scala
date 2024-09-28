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
import io.smartdatalake.config.{ConfigurationException, FromConfigFactory, InstanceRegistry}
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.action.DataFrameActionImpl
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataobject.TableDataObject
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Returns dataframe with only unique columns based on a primary key.
 *
 * If primaryKeyColumns is left empty, the primaryKey of the Actions output DataObject must be defined.
 *
 * @param name              Name of the transformer
 * @param description       Optional description of the transformer
 * @param rankingExpression Ranking expression to choose the record to keep if there are duplicates.
 * @param primaryKeyColumns Optional list of primary key columns.
 *                          If left empty the primary key of the Actions output DataObject is used.
 */
case class DeduplicateTransformer(override val name: String = "DeduplicateTransformer", override val description: Option[String] = None, rankingExpression: String, primaryKeyColumns: Option[Seq[String]] = Option.empty[Seq[String]]) extends GenericDfTransformer {

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {

    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._

    val primaryKey: Seq[String] = primaryKeyColumns.orElse {
      val action = Environment.instanceRegistry.get[DataFrameActionImpl](actionId)
      action.mainOutput match {
        case dataObject: TableDataObject => dataObject.table.primaryKey
        case _ => None
      }
    }.getOrElse(throw ConfigurationException("There are no primary key columns defined ether by parameter nor by detection with actionId."))

    df.withColumn("_rank", window(() => row_number, primaryKey.map(col), expr(rankingExpression).desc))
      .where(col("_rank") === lit(1))
      .drop("_rank")
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = DeduplicateTransformer
}

object DeduplicateTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): DeduplicateTransformer = {
    extract[DeduplicateTransformer](config)
  }
}