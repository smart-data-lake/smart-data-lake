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
import io.smartdatalake.workflow.dataobject.DataObject
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

/**
 * Interface to define a custom Spark-Dataset transformation (2:1)
 * When you implement this interface, you need to provide 3 case classes: 2 for your input Datasets
 * and 1 for your output Dataset.
 */
trait CustomDsNto1Transformer extends Serializable {
  val expectedTransformMessage = "CustomDsNTo1Transformer implementations need to implement exactly one method with name 'transform' with this signature:" +
    s"def transform(session: SparkSession, options: Map[String, String], src1DS: Dataset[A], src2DS: Dataset[B], <more Datasets if needed>): Dataset[C]"

  private[smartdatalake] def transformWithParamMapping(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame], inputDOs: Seq[DataObject], useInputDOOrdering: Boolean): DataFrame = {

    // lookup transform method
    val methodName = "transform"
    val mirror = scala.reflect.runtime.currentMirror
    val typeTagSubclass = mirror.classSymbol(this.getClass).toType
    val transformMethodsOfSubclass = typeTagSubclass.members.filter(_.isMethod).filter(_.name.toString == methodName)
    assert(transformMethodsOfSubclass.size == 1, expectedTransformMessage)
    val transformMethod = transformMethodsOfSubclass.head

    // prepare parameters.
    // We need to remember the original order of the method parameters to make sure to keep them in the same order in the output
    val transformParameters: List[(universe.Symbol, Int)] = transformMethod.info.paramLists.head.zipWithIndex
    val datasetParams = transformParameters.filter(param => param._1.typeSignature <:< typeOf[Dataset[_]])
    val nonDatasetParams = transformParameters.filterNot(param => param._1.typeSignature <:< typeOf[Dataset[_]])

    val mappedNonDatasetParams: List[(Int, Object)] = nonDatasetParams.map {
      case (param, paramIndex) if param.typeSignature =:= typeOf[SparkSession] =>
        (paramIndex, session)
      case (param, paramIndex) if param.typeSignature =:= typeOf[Map[String, String]] =>
        (paramIndex, options)
      case (param, paramIndex) => throw new IllegalStateException(s"Transform method parameter $param at index $paramIndex has unsupported type ${param.typeSignature.typeSymbol.name.toString}. Only parameters of type Dataset, SparkSession and Map[String,String] are allowed. " + expectedTransformMessage)
    }

    val mappedDatasetParams: List[(Int, Dataset[_])] =
      if (useInputDOOrdering)
        getMappedDatasetParamsBasedOnOrdering(datasetParams, inputDOs, dfs)
      else
        getMappedDatasetParamsBasedOnDataObjectId(datasetParams, dfs)

    //Sort by original parameterIndex and then forget about the index
    val allMappedParams: List[Object] = (mappedNonDatasetParams ++ mappedDatasetParams).sortBy(_._1).map(_._2)

    // call method dynamically
    val transformMethodInstance = getClass.getMethods.find(_.getName == methodName).get
    val res = transformMethodInstance.invoke(this, allMappedParams: _*)
    res.asInstanceOf[Dataset[_]].toDF
  }

  private def getMappedDatasetParamsBasedOnOrdering(datasetParamsWithParamIndex: List[(universe.Symbol, Int)], inputDOs: Seq[DataObject], dfs: Map[String, DataFrame]) = {
    assert(datasetParamsWithParamIndex.size == inputDOs.size, s"Number of Dataset-Parameters of transform function does not much number of input DataObjects! datasetParamsWithParamIndex: $datasetParamsWithParamIndex, inputDOs: ${inputDOs.map(_.id).mkString(",")}")
    datasetParamsWithParamIndex.zipWithIndex.map {
      case ((param, paramIndex), datasetIndex) if param.typeSignature <:< typeOf[Dataset[_]] =>
        val dsType = param.typeSignature.typeArgs.head
        val dataObjectAtThatIndexInConfig = inputDOs(datasetIndex)
        val df = dfs(dataObjectAtThatIndexInConfig.id.id)
        (paramIndex, EncoderUtil.createDataset(df, dsType))
    }
  }

  private def getMappedDatasetParamsBasedOnDataObjectId(datasetParamsWithParamIndex: List[(universe.Symbol, Int)], dfs: Map[String, DataFrame]) = {
    datasetParamsWithParamIndex.map {
      case (param, paramIndex) if param.typeSignature <:< typeOf[Dataset[_]] =>
        val paramName = param.name.toString
        val dsType = param.typeSignature.typeArgs.head
        val df = dfs.getOrElse(paramName, throw new IllegalStateException(s"DataFrame for DataObject $paramName not found in input DataFrames"))
        (paramIndex, EncoderUtil.createDataset(df, dsType))
    }
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