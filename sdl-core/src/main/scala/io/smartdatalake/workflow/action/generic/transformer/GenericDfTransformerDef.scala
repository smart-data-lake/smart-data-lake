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

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigHolder, ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.action.generic.transformer.OptionsGenericDfTransformer.PREVIOUS_TRANSFORMER_NAME
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.DataFrame

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

trait PartitionValueTransformer {
  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   * @param actionId id of the action which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @return Map of input to output partition values. This allows to map partition values forward and backward, which is needed in execution modes. Return None if mapping is 1:1.
   */
  def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = None

  private[smartdatalake] def applyTransformation(actionId: ActionId, partitionValuesMap: Map[PartitionValues,PartitionValues], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    val thisPartitionValuesMap = transformPartitionValues(actionId, partitionValuesMap.values.toStream.distinct, executionModeResultOptions) // note that stream is lazy -> distinct is only calculated if transformPartitionValues creates a mapping.
    thisPartitionValuesMap.map(newMapping => partitionValuesMap.mapValues(newMapping))
      .getOrElse(partitionValuesMap)
  }
}

/**
 * Interface to implement GenericDataFrame transformers working with one input and one output (1:1)
 * Note that this interface cannot be parsed from config, it's only used for programmatically defined transformers.
 * Check GenericDfTransformer trait to implement transformers that should be parsed from config.
 */
trait GenericDfTransformerDef extends PartitionValueTransformer {
  def name: String
  def description: Option[String]
  /**
   * Optional function to implement validations in prepare phase.
   */
  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = Unit
  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   */
  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame

  /**
   * Declare supported Language for transformation.
   * Can be DataFrameSubFeed to work with GenericDataFrame, or SparkSubFeed to work with Spark-DataFrames
   */
  private[smartdatalake] def getSubFeedSupportedType: Type = typeOf[DataFrameSubFeed]

  private[smartdatalake] def applyTransformation(actionId: ActionId, subFeed: DataFrameSubFeed, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    val transformedDf = subFeed.dataFrame.map(df => transform(actionId, subFeed.partitionValues, df, subFeed.dataObjectId, previousTransformerName, executionModeResultOptions))
    val transformedPartitionValues = transformPartitionValues(actionId, subFeed.partitionValues, executionModeResultOptions).map(_.values.toSeq.distinct)
      .getOrElse(subFeed.partitionValues)
    subFeed.withDataFrame(transformedDf).withPartitionValues(transformedPartitionValues)
  }
}

/**
 * Interface to implement GenericDataFrame transformers working with one input and one output (1:1)
 */
trait GenericDfTransformer extends GenericDfTransformerDef with ParsableFromConfig[GenericDfTransformer] with ConfigHolder

/**
 * Interface to implement Spark-DataFrame transformers working with one input and one output (1:1)
 */
trait SparkDfTransformer extends GenericDfTransformer {
  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame
  final override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    df match {
      case sparkDf: SparkDataFrame => SparkDataFrame(transform(actionId, partitionValues, sparkDf.inner, dataObjectId))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transform")
    }
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}

/**
 * Interface to implement GenericDataFrame transformers working with one input and one output (1:1) and options.
 * This trait extends GenericDfTransformerDef to pass a map of options as parameter to the transform function.
 * This is mainly used by custom transformers.
 */
trait OptionsGenericDfTransformer extends GenericDfTransformer {
  def options: Map[String,String]
  def runtimeOptions: Map[String,String]

  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, options: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = None

  final override def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformPartitionValuesWithOptions(actionId, partitionValues, options ++ runtimeOptionsReplaced ++ executionModeResultOptions)
  }
  final override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformWithOptions(actionId, partitionValues, df, dataObjectId, options ++ runtimeOptionsReplaced ++ executionModeResultOptions ++ previousTransformerName.map(PREVIOUS_TRANSFORMER_NAME -> _))
  }
  private def prepareRuntimeOptions(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[String,String] = {
    lazy val data = DefaultExpressionData.from(context, partitionValues)
    runtimeOptions.mapValues {
      expr => SparkExpressionUtil.evaluateString(actionId, Some(s"transformations.$name.runtimeOptions"), expr, data)
    }.filter(_._2.isDefined).mapValues(_.get)
  }
}
object OptionsGenericDfTransformer {
  private[smartdatalake] val PREVIOUS_TRANSFORMER_NAME = "previousTransformerName"
}

/**
 * Interface to implement Spark-DataFrame transformers working with one input and one output (1:1) and options.
 * This trait extends OptionsGenericDfTransformer and passes a map of options as parameter to the transform function.
 * This is mainly used by custom transformers.
 */
trait OptionsSparkDfTransformer extends OptionsGenericDfTransformer {
  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String,String])(implicit context: ActionPipelineContext): DataFrame
  final override def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: DataObjectId, options: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    df match {
      case sparkDf: SparkDataFrame => SparkDataFrame(transformWithOptions(actionId, partitionValues, sparkDf.inner, dataObjectId, options))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transformWithOptions")
    }
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}

/**
 * Legacy wrapper for pure Spark-DataFrame transformation function
 */
case class SparkDfTransformerFunctionWrapper(override val name: String, fn: DataFrame => DataFrame) extends GenericDfTransformerDef {
  override val description: Option[String] = None
  override def transform(actionId: SdlConfigObject.ActionId, partitionValues: Seq[PartitionValues], df: GenericDataFrame, dataObjectId: SdlConfigObject.DataObjectId, previousTransformerName: Option[String], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): GenericDataFrame = {
    df match {
      case sparkDf: SparkDataFrame => SparkDataFrame(fn(sparkDf.inner))
      case _ => throw new IllegalStateException(s"($actionId) Unsupported subFeedType ${df.subFeedType.typeSymbol.name} in method transform")
    }
  }
  override def getSubFeedSupportedType: universe.Type = typeOf[SparkSubFeed]
}


















