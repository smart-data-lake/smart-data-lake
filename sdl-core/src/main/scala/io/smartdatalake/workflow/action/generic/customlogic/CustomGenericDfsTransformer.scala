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
package io.smartdatalake.workflow.action.generic.customlogic

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{DynamicTransformContext, TransformParameterMapper, TransformReturnMapper}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Interface to define a custom Spark-DataFrame transformation (n:m)
 * Same trait as [[CustomGenericDfTransformer]], but multiple input and outputs supported.
 *
 * There are two methods to define the transformation:
 *
 * 1) Overwrite the generic transform function below: Define a transform function which receives a map of input
 * DataObjectIds with GenericDataFrames and a map of options and has to return a map of output DataObjectIds with
 * GenericDataFrames.
 *
 * 2) Implement any transform method using parameters of type DataFrameFunctions, Map[String,String],
 * GenericDataFrame and any primitive data type (String, Boolean, Int, ...). Primitive data types might also use
 * default values or be enclosed in an Option[...] to mark it as non required. The transform method is then called
 * dynamically by looking for the parameter values in the input DataFrames and Options.
 */
trait CustomGenericDfsTransformer extends DynamicTransform with Serializable {

  /**
   * Function to define the transformation between several input and output GenericDataFrames (n:m)
   *
   * Note that the default implementation is looking for an implementation of a 'transform' function with custom
   * parameters, which it will call dynamically.
   *
   * @param helper  Functions to work with type of the GenericDataFrames given.
   * @param options Options specified in the configuration for this transformation
   * @param dfs     GenericDataFrames to be transformed
   * @return Transformed GenericDataFrame
   */
  def transform(helper: DataFrameFunctions, options: Map[String, String], dfs: Map[String, GenericDataFrame]): Map[String, GenericDataFrame] = {
    callDynamicTransform(DynamicTransformContext(dfs = dfs, options = options, engineObjects = Seq(helper)))
      .view.mapValues(_.asInstanceOf[GenericDataFrame]).toMap
  }

  override protected def stdTransformMethodSignature: universe.Type = CustomGenericDfsTransformer.stdTransformMethodSignature
  override protected def transformMethodHelpMsg: String = CustomGenericDfsTransformer.transformMethodHelpMsg
  override protected def transformParameterMappers: Seq[TransformParameterMapper] = GenericTransformMappers.parameterMappers
  override protected def transformReturnMapper: TransformReturnMapper = GenericTransformMappers.GenericReturnMapper

  /**
   * Optional function to define the transformation of input to output partition values.
   * Use cases:
   * - implement aggregations where multiple input partitions are combined into one output partition
   * - add additional fixed partition values to write from different actions into the same target tables but separated by different partition values
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param partitionValues partition values to be transformed
   * @param options         Options specified in the configuration for this transformation
   * @return a map of input partition values to output partition values
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] = None

}

object CustomGenericDfsTransformer {
  private[smartdatalake] val stdTransformMethodSignature: universe.Type =
    typeOf[CustomGenericDfsTransformer].members.find(_.name.toString == "transform").head.typeSignature
  private[smartdatalake] val transformMethodHelpMsg: String =
    """
      | CustomGenericDfsTransformer implementations need to implement one method with name 'transform'.
      | Traditionally the signature of the transform method is 'transform(helper: DataFrameFunctions, options: Map[String,String], dfs: Map[String,GenericDataFrame]): Map[String,GenericDataFrame]',
      | but you can also implement any transform method using parameters of type DataFrameFunctions, Map[String,String], GenericDataFrame and any primitive data type (String, Boolean, Int, ...).
      | Primitive data types might also use default values or be enclosed in an Option[...] to mark it as non required.
      | The transform method is then called dynamically by looking for the parameter values in the input DataFrames and Options.
    """.stripMargin
}
