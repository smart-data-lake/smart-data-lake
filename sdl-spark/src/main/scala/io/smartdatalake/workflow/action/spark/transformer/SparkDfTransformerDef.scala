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

import io.smartdatalake.config.SdlConfigObject
import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.ActionPipelineContext
import io.smartdatalake.workflow.action.generic.transformer.{GenericDfTransformer, GenericDfTransformerDef, OptionsGenericDfTransformer}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Spark-specific transformer interface working with one input and one output (1:1).
 * Implement transform to work directly with Spark DataFrames instead of the generic
 * GenericDataFrame abstraction.
 */
trait SparkDfTransformer extends GenericDfTransformer {
  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame
  final override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    df match {
      case sparkDf: SparkDataFrame => SparkDataFrame(transform(actionId, partitionValues, sparkDf.inner, dataObjectId))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transform")
    }
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}

/**
 * Spark-specific transformer interface working with one input and one output (1:1) and options.
 * Implement transformWithOptions to work directly with Spark DataFrames and receive the merged
 * options map.
 */
trait OptionsSparkDfTransformer extends OptionsGenericDfTransformer {
  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): DataFrame
  final override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, options: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    df match {
      case sparkDf: SparkDataFrame => SparkDataFrame(transformWithOptions(actionId, partitionValues, sparkDf.inner, dataObjectId, options))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transformWithOptions")
    }
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}

/**
 * Legacy wrapper for pure Spark-DataFrame transformation function.
 */
case class SparkDfTransformerFunctionWrapper(override val name: String, fn: DataFrame => DataFrame) extends GenericDfTransformerDef {
  override val description: Option[String] = None
  override def transform(actionId: SdlConfigObject.ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: SdlConfigObject.DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String, String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    df match {
      case sparkDf: SparkDataFrame => SparkDataFrame(fn(sparkDf.inner))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transform")
    }
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}
