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
package io.smartdatalake.workflow.action.spark.customlogic

import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.spark.EncoderUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe.typeOf

/**
 * Interface to define a custom Spark-Dataset transformation (1:1)
 * When you implement this interface, you need to provide two case classes: One for your input Dataset
 * and one for your output Dataset.
 */
trait CustomDsNto1Transformer extends Serializable {

  private[smartdatalake] def transformBasedOnDataObjectId(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): DataFrame = {

    // lookup transform method
    val methodName = "transform"
    val mirror = scala.reflect.runtime.currentMirror
    val typeTagSubclass = mirror.classSymbol(this.getClass).toType
    val transformMethodsOfSubclass = typeTagSubclass.members.filter(_.isMethod).filter(_.name.toString == methodName)
    assert(transformMethodsOfSubclass.size == 1, s"Transform method not unique. CustomDsNTo1Transformer implementations are only allowed to implement one method with name 'transform'.")
    val transformMethod = transformMethodsOfSubclass.head

    // prepare parameters
    val transformParameters = transformMethod.info.paramLists.head
    val transformParameterInstances = transformParameters.map {
      case param if param.typeSignature <:< typeOf[Dataset[_]] =>
        val paramName = param.name.toString
        val dsType = param.typeSignature.typeArgs.head
        val df = dfs.getOrElse(paramName, throw new IllegalStateException(s"DataFrame for DataObject $paramName not found in input DataFrames"))
        EncoderUtil.createDataset(df, dsType)
      case param if param.typeSignature =:= typeOf[SparkSession] =>
        session
      case param if param.typeSignature =:= typeOf[Map[String,String]] =>
        options
      case param => throw new IllegalStateException(s"Transform method parameter with unallowed parameter type ${param.typeSignature.typeSymbol.name.toString} found. Only parameters of type Dataset, SparkSession and Map[String,String] are allowed.")
    }

    // call method dynamically
    val transformMethodInstance = getClass.getMethods.find(_.getName == methodName).get
    transformMethodInstance.invoke(this, transformParameterInstances:_*).asInstanceOf[Dataset[_]].toDF
  }

  /**
   * Optional function to define the transformation of input to output partition values.
   * For example this enables to implement aggregations where multiple input partitions are combined into one output partition.
   * Note that the default value is input = output partition values, which should be correct for most use cases.
   *
   * @param partitionValues partition values to be transformed
   * @param options         Options specified in the configuration for this transformation
   * @return Map of input to output partition values. This allows to map partition values forward and backward, which is needed in execution modes.
   *         Return None if mapping is 1:1.
   */
  def transformPartitionValues(options: Map[String, String], partitionValues: Seq[PartitionValues]): Option[Map[PartitionValues, PartitionValues]] = None
}