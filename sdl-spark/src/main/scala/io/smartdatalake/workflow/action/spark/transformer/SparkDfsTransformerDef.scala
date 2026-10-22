/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.workflow.action.spark.transformer

import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfsTransformer, OptionsGenericDfsTransformer}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Spark-specific transformer interface working with many inputs and many outputs (n:m). Implement
 * transformSpark to work directly with Spark DataFrames instead of the generic GenericDataFrame
 * abstraction.
 */
trait SparkDfsTransformer extends GenericDfsTransformer {
  def transformSpark(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, DataFrame])(implicit
      context: ActionPipelineContext
  ): Map[String, DataFrame]

  final override def transform(
      actionId: ActionId,
      partitionValues: Seq[PartitionValues],
      dfs: Map[String, GenericDataFrame],
      executionModeResultOptions: Map[String, String],
      outputDataObjectIds: Seq[String]
  )(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    assert(
      dfs.values.forall(_.isInstanceOf[SparkDataFrame]),
      s"($actionId) Unsupported subFeedType(s)" +
        s" ${dfs.values.filterNot(_.isInstanceOf[SparkDataFrame]).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")}" +
        s" in method transform"
    )
    val sparkDfs = dfs.view.mapValues(_.asInstanceOf[SparkDataFrame].inner).toMap
    transformSpark(actionId, partitionValues, sparkDfs)
      .view.mapValues(SparkDataFrame(_)).toMap
  }

  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}

/**
 * Spark-specific transformer interface working with many inputs and many outputs (n:m), with
 * options. Implement transformSparkWithOptions to work directly with Spark DataFrames and receive
 * the merged options map.
 */
trait OptionsSparkDfsTransformer extends OptionsGenericDfsTransformer {
  def transformSparkWithOptions(
      actionId: ActionId,
      partitionValues: Seq[PartitionValues],
      dfs: Map[String, DataFrame],
      options: Map[String, String]
  )(implicit context: ActionPipelineContext): Map[String, DataFrame]

  override def transformWithOptions(
      actionId: ActionId,
      partitionValues: Seq[PartitionValues],
      dfs: Map[String, GenericDataFrame],
      options: Map[String, String]
  )(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    assert(
      dfs.values.forall(_.isInstanceOf[SparkDataFrame]),
      s"($actionId) Unsupported subFeedType(s) ${dfs.values.filterNot(_.isInstanceOf[SparkDataFrame]).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method transform"
    )
    val sparkDfs = dfs.view.mapValues(_.asInstanceOf[SparkDataFrame].inner).toMap
    transformSparkWithOptions(actionId, partitionValues, sparkDfs, options)
      .view.mapValues(SparkDataFrame(_)).toMap
  }

  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}
