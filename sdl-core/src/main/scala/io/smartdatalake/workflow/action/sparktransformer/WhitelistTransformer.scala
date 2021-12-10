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

package io.smartdatalake.workflow.action.sparktransformer

import com.typesafe.config.Config
import io.smartdatalake.config.{FromConfigFactory, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.ActionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Apply a column whitelist to a DataFrame.
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param columnWhitelist List of columns to keep from DataFrame
 */

case class WhitelistTransformer(override val name: String = "whitelist", override val description: Option[String] = None, columnWhitelist: Seq[String]) extends ParsableDfTransformer {
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit session: SparkSession, context: ActionPipelineContext): DataFrame = {
    ActionHelper.filterWhitelist(columnWhitelist)(df)
  }
  override def factory: FromConfigFactory[ParsableDfTransformer] = WhitelistTransformer
}

object WhitelistTransformer extends FromConfigFactory[ParsableDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): WhitelistTransformer = {
    extract[WhitelistTransformer](config)
  }
}