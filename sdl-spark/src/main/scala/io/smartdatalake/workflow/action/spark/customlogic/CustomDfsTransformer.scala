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
import io.smartdatalake.util.misc._
import io.smartdatalake.workflow.action.generic.customlogic.{DynamicTransform, TransformInfo}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Interface to define a custom Spark-DataFrame transformation (n:m)
 * Same trait as [[CustomDfTransformer]], but multiple input and outputs supported.
 */
trait CustomDfsTransformer extends DynamicTransform with TransformInfo with TransformDfsMethod with Serializable with SmartDataLakeLogger {

  /**
   * Function to define the transformation between several input and output DataFrames (n:m).
   *
   * Note that the default implementation is looking for an implementation of a 'transform' function with custom parameters,
   * which it will call dynamically.
   *
   * @param session Spark Session
   * @param options Options specified in the configuration for this transformation
   * @param dfs DataFrames to be transformed
   * @return Map of transformed DataFrames
   */
  override def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame] = {
    callDynamicTransform(DynamicTransformContext(dfs = dfs, options = options, engineObjects = Seq(session)))
      .view.mapValues(_.asInstanceOf[DataFrame]).toMap
  }

  /**
   * Optional function to define the transformation of input to output partition values.
   * Use cases:
   * - implement aggregations where multiple input partitions are combined into one output partition
   * - add additional fixed partition values to write from different actions into the same target tables but separated by different partition values
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param partitionValues partition values to be transformed
   * @param options Options specified in the configuration for this transformation
   * @return a map of input partition values to output partition values
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues,PartitionValues]] = None

  override protected def stdTransformMethodSignature: universe.Type = CustomDfsTransformer.stdTransformMethodSignature
  override protected def transformMethodHelpMsg: String = CustomDfsTransformer.transformMethodHelpMsg
  override protected def transformParameterMappers: Seq[TransformParameterMapper] = SparkTransformMappers.parameterMappers
  override protected def transformReturnMapper: TransformReturnMapper = SparkTransformMappers.SparkReturnMapper

  override def getInputDataObjectsNameAndType: Option[Seq[(String, universe.Type)]] = customTransformMethodWrapper
    .map(_.getInputDataFrameNames.view.mapValues(_.tpe).toSeq)
  override def isSingleInput: Boolean = customTransformMethodWrapper.exists(_.getInputDataFrameNames.keys.size==1)
  override def isSingleOutput: Boolean = customTransformMethodWrapper.exists(_.returnsSingleDataFrame)
}

object CustomDfsTransformerConfig {
  type fnTransformType = (SparkSession, Map[String,String], Map[String,DataFrame]) => Map[String,DataFrame]
}

object CustomDfsTransformer {
  private[smartdatalake] val stdTransformMethodSignature: universe.Type =
    typeOf[CustomDfsTransformer].members.find(_.name.toString == "transform").head.typeSignature
  private[smartdatalake] val transformMethodHelpMsg: String =
    """
      | CustomDfsTransformer implementations need to implement one method with name 'transform'.
      | Traditionally the signature of the transform method is 'transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]): Map[String,DataFrame]',
      | but since SDLB 2.6 you can also implement any transform method using parameters of type SparkSession, Map[String,String], DataFrame, Dataset[<Product>] and any primitive data type (String, Boolean, Int, ...).
      | Primitive data types might also use default values or be enclosed in an Option[...] to mark it as non required.
      | The transform method is then called dynamically by looking for the parameter values in the input DataFrames and Options.
    """.stripMargin
}

/**
 * A trait to define the method to transform m:n DataFrames
 */
trait TransformDfsMethod {
  def transform(session: SparkSession, options: Map[String,String], dfs: Map[String,DataFrame]) : Map[String,DataFrame]
}
