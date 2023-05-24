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
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{SQLUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

/**
 * Apply a column blacklist to a DataFrame.
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

    val colsToSelect =
      if (Environment.caseSensitive) filterColumnsCaseSensitive(df)
      else filterColumnsCaseInsensitive(df)
    df.select(colsToSelect.map(SQLUtil.sparkQuoteSQLIdentifier).map(col))
  }

  private def filterColumnsCaseInsensitive(df: GenericDataFrame): Seq[String] = {
    val lowerCaseSchemaColumns = df.schema.columns.map(_.toLowerCase()).toSet
    val nonExistingColumns = columnBlacklist.filter(colName => !lowerCaseSchemaColumns.contains(colName.toLowerCase()))
    if (nonExistingColumns.nonEmpty) {
      logNonExistingColumns(nonExistingColumns, df)
    }
    val lowerCaseBlacklistColumns = columnBlacklist.map(_.toLowerCase()).toSet
    df.schema.columns.filter(colName => !lowerCaseBlacklistColumns.contains(colName.toLowerCase()))
  }

  private def filterColumnsCaseSensitive(df: GenericDataFrame): Seq[String] = {
    val schemaColumnsSet = df.schema.columns.toSet
    val blacklistColumnsSet = columnBlacklist.toSet
    val nonExistingColumns = blacklistColumnsSet -- schemaColumnsSet
    if (nonExistingColumns.nonEmpty) {
      logNonExistingColumns(nonExistingColumns, df)
    }
    (schemaColumnsSet -- blacklistColumnsSet).toSeq
  }

  private def logNonExistingColumns(nonExistingColumns: Iterable[String], df: GenericDataFrame): Unit = {
    logger.warn(s"The blacklisted columns [${nonExistingColumns.mkString(", ")}] do not exist in dataframe. " +
      s"Available columns are [${df.schema.columns.mkString(", ")}].")
  }

  override def factory: FromConfigFactory[GenericDfTransformer] = BlacklistTransformer
}

object BlacklistTransformer extends FromConfigFactory[GenericDfTransformer] {
  override def fromConfig(config: Config)(implicit instanceRegistry: InstanceRegistry): BlacklistTransformer = {
    extract[BlacklistTransformer](config)
  }
}