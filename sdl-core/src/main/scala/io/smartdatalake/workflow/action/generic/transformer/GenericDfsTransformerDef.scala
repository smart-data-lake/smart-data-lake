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

import io.smartdatalake.config.ParsableFromConfig
import io.smartdatalake.config.SdlConfigObject.ActionId
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}


/**
 * Interface to implement GenericDataFrame transformers working with many inputs and many outputs (n:m)
 * Note that this interface cannot be parsed from config, it's only used for programmatically defined transformers.
 * Check GenericDfsTransformer trait to implement transformers that should be parsed from config.
 */
trait GenericDfsTransformerDef extends PartitionValueTransformer {
  def name: String
  def description: Option[String]

  /**
   * Optional function to implement validations in prepare phase.
   */
  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = Unit

  /**
   * Function to be implemented to define the transformation between many inputs and many outputs (n:m)
   * @param actionId id of the action which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @param dfs Map of (dataObjectId, DataFrame) tuples available as input
   * @return Map of transformed (dataObjectId, DataFrame) tuples
   */
  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame]

  /**
   * Declare supported Language for transformation.
   * Can be DataFrameSubFeed to work with GenericDataFrame, or SparkSubFeed to work with Spark-DataFrames
   */
  private[smartdatalake] def getSubFeedSupportedType: Type = typeOf[DataFrameSubFeed]

  private[smartdatalake] def applyTransformation(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame])(implicit context: ActionPipelineContext): (Map[String,GenericDataFrame], Seq[PartitionValues]) = {
    val transformedDfs = transform(actionId, partitionValues, dfs)
    val transformedPartitionValues = transformPartitionValues(actionId, partitionValues).map(_.values.toSeq.distinct)
      .getOrElse(partitionValues)
    (transformedDfs,transformedPartitionValues)
  }
}

/**
 * Interface to implement Spark-DataFrame transformers working with many inputs and many outputs (n:m)
 */
trait GenericDfsTransformer extends GenericDfsTransformerDef with ParsableFromConfig[GenericDfsTransformer]

trait SparkDfsTransformer extends GenericDfsTransformer {
  // Note: must have a different name as transform because signature is different only in subtypes of parameters.
  def transformSpark(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, DataFrame])(implicit context: ActionPipelineContext): Map[String, DataFrame]
  final override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String, GenericDataFrame])(implicit context: ActionPipelineContext): Map[String, GenericDataFrame] = {
    assert(dfs.values.forall(_.isInstanceOf[SparkDataFrame]), s"($actionId) Unsupported subFeedType(s) ${dfs.values.filterNot(_.isInstanceOf[SparkDataFrame]).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method transform")
    val sparkDfs = dfs.mapValues(_.asInstanceOf[SparkDataFrame].inner)
    transformSpark(actionId, partitionValues, sparkDfs)
      .mapValues(SparkDataFrame)
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}

/**
 * Interface to implement Spark-DataFrame transformers working with many inputs and many outputs (n:m)
 * This trait extends DfSparkTransformer to pass a map of options as parameter to the transform function. This is mainly
 * used by custom transformers.
 */
trait OptionsGenericDfsTransformer extends GenericDfsTransformer {
  def options: Map[String,String]
  def runtimeOptions: Map[String,String]

  /**
   * Function to be implemented to define the transformation between many inputs and many outputs (n:m)
   * see also [[GenericDfsTransformerDef.transform()]]
   *
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], options: Map[String,String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame]

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   * see also [[GenericDfsTransformerDef.transformPartitionValues()]]
   *
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = None

  override def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformPartitionValuesWithOptions(actionId, partitionValues, options ++ runtimeOptionsReplaced)
  }
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformWithOptions(actionId, partitionValues, dfs, options ++ runtimeOptionsReplaced )
  }
  private def prepareRuntimeOptions(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[String,String] = {
    lazy val data = DefaultExpressionData.from(context, partitionValues)
    runtimeOptions.mapValues {
      expr => SparkExpressionUtil.evaluateString(actionId, Some(s"transformations.$name.runtimeOptions"), expr, data)
    }.filter(_._2.isDefined).mapValues(_.get)
  }
}

/**
 * Interface to implement Spark-DataFrame transformers working with multiple inputs and outputs (n:m) and options.
 * This trait extends OptionsGenericDfsTransformer and passes a map of options as parameter to the transform function.
 * This is mainly used by custom transformers.
 */
trait OptionsSparkDfsTransformer extends OptionsGenericDfsTransformer {
  // Note: must have a different name as transform because signature is different only in subtypes of parameters.
  def transformSparkWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,DataFrame], options: Map[String,String])(implicit context: ActionPipelineContext): Map[String,DataFrame]
  override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], options: Map[String,String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    assert(dfs.values.forall(_.isInstanceOf[SparkDataFrame]), s"($actionId) Unsupported subFeedType(s) ${dfs.values.filterNot(_.isInstanceOf[SparkDataFrame]).map(_.subFeedType.typeSymbol.name).toSet.mkString(", ")} in method transform")
    val sparkDfs = dfs.mapValues(_.asInstanceOf[SparkDataFrame].inner)
    transformSparkWithOptions(actionId, partitionValues, sparkDfs, options)
      .mapValues(SparkDataFrame)
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}