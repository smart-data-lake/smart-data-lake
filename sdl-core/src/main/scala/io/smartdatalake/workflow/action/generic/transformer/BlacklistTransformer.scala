/*
 * Smart Data Lake Builder - Build your data lake the smart way.
 *
 * Copyright © 2019-2026 ELCA Informatique SA (<https://www.elca.ch>)
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
import io.smartdatalake.util.misc.{SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Apply a column blacklist to a DataFrame: every column listed in `columnBlacklist` is dropped, all
 * remaining columns are passed through unchanged.
 * Use this if a source delivers many columns and only a few need to be removed, e.g. to strip technical
 * or sensitive attributes before writing them to the target DataObject.
 * Use [[WhitelistTransformer]] instead if it is easier to enumerate the columns to keep.
 *
 * Column names are matched case-insensitively, unless case sensitivity is enabled globally by setting
 * `global.environment.caseSensitive = true`. Blacklisted columns that do not exist in the DataFrame do not
 * fail the Action, they are logged as a warning and ignored.
 * Note that the original column order is only preserved in the default case-insensitive mode; with case
 * sensitivity enabled the order of the remaining columns is undefined.
 *
 * Example:
 * {{{
 * actions = {
 *   load-orders {
 *     type = CopyAction
 *     inputId = stg-orders
 *     outputId = int-orders
 *     transformers = [{
 *       type = BlacklistTransformer
 *       columnBlacklist = [dl_load_ts, source_system, customer_email]
 *     }]
 *   }
 * }
 * }}}
 *
 * @param name         name of the transformer
 * @param description  Optional description of the transformer
 * @param columnBlacklist List of columns to exclude from DataFrame
 */
case class BlacklistTransformer(override val name: String = "blacklist", override val description: Option[String] = None, columnBlacklist: Seq[String])
  extends GenericDfTransformer with SmartDataLakeLogger {

  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId,
                         previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])
                        (implicit context: ActionPipelineContext): GenericDataFrame = {
    val functions = DataFrameSubFeed.getFunctions(df.subFeedType)
    import functions._

    val colsToSelect = df.schema.filterColumns(columnBlacklist, includeColumns = false)
    df.select(colsToSelect.map(SQLUtil.sparkQuoteSQLIdentifier).map(col))
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = BlacklistTransformer
}

object BlacklistTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BlacklistTransformer = {
    extract[BlacklistTransformer](config)
  }
}