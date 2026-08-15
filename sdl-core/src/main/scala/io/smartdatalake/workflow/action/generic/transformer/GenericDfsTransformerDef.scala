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

import io.smartdatalake.config.SdlConfigObject.{ActionId, DataObjectId}
import io.smartdatalake.config.{ConfigHolder, ParsableFromConfig}
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{DefaultExpressionData, ExpressionUtil}
import io.smartdatalake.workflow.action.generic.transformer.OptionsGenericDfsTransformer.IS_EXEC
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}

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
  def prepare(actionId: ActionId)(implicit context: ActionPipelineContext): Unit = ()

  /**
   * Function to be implemented to define the transformation between many inputs and many outputs (n:m)
   * @param actionId id of the action which executes this transformation. This is mainly used to prefix error messages.
   * @param partitionValues partition values to transform
   * @param dfs Map of (dataObjectId, DataFrame) tuples available as input
   * @param executionModeResultOptions options set by the actions execution mode
   * @return Map of transformed (dataObjectId, DataFrame) tuples
   */
  def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], executionModeResultOptions: Map[String,String], outputDataObjectIds: Seq[String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame]

  /**
   * Declare supported Language for transformation.
   * Can be DataFrameSubFeed to work with GenericDataFrame, or SparkSubFeed to work with Spark-DataFrames
   */
  private[smartdatalake] def getSubFeedSupportedType: Type = typeOf[DataFrameSubFeed]

  private[smartdatalake] def applyTransformation(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], executionModeResultOptions: Map[String,String], outputDataObjectIds: Seq[DataObjectId])(implicit context: ActionPipelineContext): (Map[String,GenericDataFrame], Seq[PartitionValues]) = {
    val transformedDfs = transform(actionId, partitionValues, dfs, executionModeResultOptions, outputDataObjectIds.map(_.id))
    val transformedPartitionValues = transformPartitionValues(actionId, partitionValues, executionModeResultOptions).map(_.values.toSeq.distinct)
      .getOrElse(partitionValues)
    (transformedDfs,transformedPartitionValues)
  }
}

/**
 * Interface to implement GenericDataFrame transformers working with many inputs and many outputs (n:m)
 */
trait GenericDfsTransformer extends GenericDfsTransformerDef with ParsableFromConfig[GenericDfsTransformer] with ConfigHolder

/**
 * Interface to implement GenericDataFrame transformers working with many inputs and many outputs (n:m)
 * This trait extends GenericDfsTransformer to pass a map of options as parameter to the transform function. This is mainly
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

  override def transformPartitionValues(actionId: ActionId, partitionValues: Seq[PartitionValues], executionModeResultOptions: Map[String,String])(implicit context: ActionPipelineContext): Option[Map[PartitionValues,PartitionValues]] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // transform
    transformPartitionValuesWithOptions(actionId, partitionValues, options ++ runtimeOptionsReplaced ++ executionModeResultOptions)
  }
  override def transform(actionId: ActionId, partitionValues: Seq[PartitionValues], dfs: Map[String,GenericDataFrame], executionModeResultOptions: Map[String,String], outputDataObjectIds: Seq[String])(implicit context: ActionPipelineContext): Map[String,GenericDataFrame] = {
    // replace runtime options
    val runtimeOptionsReplaced = prepareRuntimeOptions(actionId, partitionValues)
    // prepare default options
    val defaultOptions = Seq(
      Some(IS_EXEC -> context.isExecPhase.toString),
      if (outputDataObjectIds.size == 1) Some(OptionsGenericDfsTransformer.OPTION_OUTPUT_DATAOBJECT_ID -> outputDataObjectIds.head) else None
    ).flatten.toMap
    // transform
    transformWithOptions(actionId, partitionValues, dfs, defaultOptions ++ options ++ runtimeOptionsReplaced ++ executionModeResultOptions)
  }

  private def prepareRuntimeOptions(actionId: ActionId, partitionValues: Seq[PartitionValues])
                                   (implicit context: ActionPipelineContext): Map[String, String] = {
    lazy val data = DefaultExpressionData.from(context, partitionValues)
    val evaluatedOptions = runtimeOptions.map {
      case (key, expr) => (key, expr, ExpressionUtil.evaluateString(actionId, Some(s"transformations.$name.runtimeOptions"), expr, data))
    }
    // an option whose expression evaluates to null is left undefined. Log this, as a later substitution of %{key} fails.
    evaluatedOptions.filter(_._3.isEmpty).foreach { case (key, expr, _) =>
      logger.warn(s"($actionId) runtimeOption '$key' of transformation $name is not defined," +
        s" because its expression \"$expr\" evaluated to null in phase ${context.phase}." +
        " Note that metrics of previous Actions are only available in the exec phase." +
        s" Use coalesce(<expression>, <default>) if $key should be defined in all phases.")
    }
    evaluatedOptions.collect { case (key, _, Some(value)) => (key, value) }.toMap
  }
}
object OptionsGenericDfsTransformer {
  final val IS_EXEC = "isExec"
  final val OPTION_OUTPUT_DATAOBJECT_ID = "outputDataObjectId"
}

/**
 * Interface for transformers that can recompile their code from source files.
 * This is mainly used to recompile scala class based transformers inside Notebooks.
 */
trait CanRecompileFromSrc {
  def recompileFromSrc(srcDir: String): Unit
}