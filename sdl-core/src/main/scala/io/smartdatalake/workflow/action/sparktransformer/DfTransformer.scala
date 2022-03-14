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

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigHolder, ParsableFromConfig, SdlConfigObject}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{DefaultExpressionData, SparkExpressionUtil}
import io.smartdatalake.workflow.{ActionPipelineContext, SparkSubFeed}
import org.apache.spark.sql.DataFrame

trait PartitionValueTransformer extends ConfigHolder {
  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param actionId        id of the action which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @return Map of input to output partition values. This allows to map partition values forward and backward, which is needed in execution modes. Return None if mapping is 1:1.
   */
  def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Option[Map[PartitionValues, PartitionValues]] = None

  private[smartdatalake] def applyTransformation(actionId: ActionId, partitionValuesMap: Map[PartitionValues,PartitionValues])(implicit context: ActionPipelineContext): Map[PartitionValues,PartitionValues] = {
    val thisPartitionValuesMap = transformPartitionValues(actionId, partitionValuesMap.values.toStream.distinct) // note that stream is lazy -> distinct is only calculated if transformPartitionValues creates a mapping.
    thisPartitionValuesMap.map(newMapping => partitionValuesMap.mapValues(newMapping))
      .getOrElse(partitionValuesMap)
  }
}

/**
 * Interface to implement Spark-DataFrame transformers working with one input and one output (1:1)
 */
trait DfTransformer extends PartitionValueTransformer {
  def name: String
  def description: Option[String]
  /**
   * Optional function to implement validations in prepare phase.
   */
  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = Unit
  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   */
  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame

  private[smartdatalake] def applyTransformation(actionId: ActionId, subFeed: SparkSubFeed)(implicit context: ActionPipelineContext): SparkSubFeed = {
    val transformedDf = subFeed.dataFrame.map(df => transform(actionId, subFeed.partitionValues, df, subFeed.dataObjectId))
    val transformedPartitionValues = transformPartitionValues(actionId, subFeed.partitionValues).map(_.values.toSeq.distinct)
      .getOrElse(subFeed.partitionValues)
    subFeed.copy(partitionValues = transformedPartitionValues, dataFrame = transformedDf)
  }
}
trait ParsableDfTransformer extends DfTransformer with ParsableFromConfig[ParsableDfTransformer]

/**
 * Interface to implement Spark-DataFrame transformers working with one input and one output (1:1).
 * This trait extends DfSparkTransformer to pass a map of options as parameter to the transform function. This is mainly
 * used by custom transformers.
 */
trait OptionsDfTransformer extends ParsableDfTransformer {
  def options: Map[String,String]
  def runtimeOptions: Map[String,String]

  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId, options: Map[String,String])(implicit context: ActionPipelineContext): DataFrame

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   * @param options Options specified in the configuration for this transformation, including evaluated runtimeOptions
   */
  def transformPartitionValuesWithOptions(actionId: ActionId, partitionValues: Seq[PartitionValues], options: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = None

  override def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformPartitionValuesWithOptions(actionId, partitionValues, options ++ runtimeOptionsReplaced)
  }
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformWithOptions(actionId, partitionValues, df, dataObjectId, options ++ runtimeOptionsReplaced )
  }
  private def prepareRuntimeOptions(actionId: ActionId, partitionValues: Seq[PartitionValues])(implicit context: ActionPipelineContext): Map[String,String] = {
    lazy val data = DefaultExpressionData.from(context, partitionValues)
    runtimeOptions.mapValues {
      expr => SparkExpressionUtil.evaluateString(actionId, Some(s"transformations.$name.runtimeOptions"), expr, data)
    }.filter(_._2.isDefined).mapValues(_.get)
  }
}

/**
 * Legacy wrapper for pure DataFrame transformation function
 */
case class DfTransformerFunctionWrapper(override val name: String, fn: DataFrame => DataFrame) extends DfTransformer {
  override val description: Option[String] = None
  override def transform(actionId: SdlConfigObject.ActionId, partitionValues: Seq[PartitionValues], df: DataFrame, dataObjectId: SdlConfigObject.DataObjectId)(implicit context: ActionPipelineContext): DataFrame = {
    fn(df)
  }
}


















