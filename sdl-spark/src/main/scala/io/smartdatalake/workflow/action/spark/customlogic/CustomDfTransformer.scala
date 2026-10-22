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
package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{DynamicTransformContext, TransformParameterMapper, TransformReturnMapper}
import io.smartdatalake.workflow.action.generic.customlogic.DynamicTransform
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Interface to define a custom Spark-DataFrame transformation (1:1)
 *
 * There are two methods to define the transformation:
 *
 * 1) Overwrite the transform function below: Define a transform function which receives a SparkSession, a map of
 * options, the input DataFrame and the id of its DataObject, and has to return the transformed DataFrame.
 *
 * 2) Implement any transform method using parameters of type SparkSession, Map[String,String], DataFrame,
 * Dataset[<Product>] and any primitive data type (String, Boolean, Int, ...). Primitive data types might also use
 * default values or be enclosed in an Option[...] to mark it as non required. The transform method is then called
 * dynamically by looking for the parameter values in the options. As there is exactly one input DataFrame, a
 * DataFrame or Dataset parameter is mapped to it independent of the parameters name. The id of the input DataObject
 * is available as option `dataObjectId`.
 *
 * Example:
 * {{{
 *   class ComputeDistanceTransformer extends CustomDfTransformer {
 *     def transform(df: DataFrame, unit: String = "km"): DataFrame = ...
 *   }
 * }}}
 */
trait CustomDfTransformer extends DynamicTransform with Serializable {

  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   *
   * Note that the default implementation is looking for an implementation of a 'transform' function with custom
   * parameters, which it will call dynamically.
   *
   * @param session Spark Session
   * @param options Options specified in the configuration for this transformation
   * @param df DataFrames to be transformed
   * @param dataObjectId Id of DataObject of SubFeed
   * @return Transformed DataFrame
   */
  def transform(session: SparkSession, options: Map[String, String], df: DataFrame, dataObjectId: String) : DataFrame = {
    callDynamicTransformSingleOutput(DynamicTransformContext(
      dfs = Map(dataObjectId -> df),
      options = options + (DynamicTransform.OPTION_DATAOBJECT_ID -> dataObjectId),
      engineObjects = Seq(session),
      singleInput = true,
      defaultOutputName = Some(dataObjectId)
    )).asInstanceOf[DataFrame]
  }

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param partitionValues partition values to be transformed
   * @param options Options specified in the configuration for this transformation
   * @return Map of input to output partition values. This allows to map partition values forward and backward, which is needed in execution modes.
   *         Return None if mapping is 1:1.
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues,PartitionValues]] = None

  override protected def stdTransformMethodSignature: universe.Type = CustomDfTransformer.stdTransformMethodSignature
  override protected def transformMethodHelpMsg: String = CustomDfTransformer.transformMethodHelpMsg
  override protected def transformParameterMappers: Seq[TransformParameterMapper] = SparkTransformMappers.parameterMappers
  override protected def transformReturnMapper: TransformReturnMapper = SparkTransformMappers.SparkReturnMapper
}

object CustomDfTransformer {
  private[smartdatalake] val stdTransformMethodSignature: universe.Type =
    typeOf[CustomDfTransformer].members.find(_.name.toString == "transform").head.typeSignature
  private[smartdatalake] val transformMethodHelpMsg: String =
    """
      | CustomDfTransformer implementations need to implement one method with name 'transform'.
      | Traditionally the signature of the transform method is 'transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String): DataFrame',
      | but you can also implement any transform method using parameters of type SparkSession, Map[String,String], DataFrame, Dataset[<Product>] and any primitive data type (String, Boolean, Int, ...).
      | Primitive data types might also use default values or be enclosed in an Option[...] to mark it as non required.
      | The transform method is then called dynamically by looking for the parameter values in the options.
    """.stripMargin
}

object CustomDfTransformerConfig {
  type fnTransformType = (SparkSession, Map[String, String], DataFrame, String) => DataFrame
}
