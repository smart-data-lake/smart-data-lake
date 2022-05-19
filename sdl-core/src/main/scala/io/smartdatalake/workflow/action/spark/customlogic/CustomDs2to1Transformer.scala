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
import io.smartdatalake.workflow.dataobject.DataObject
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe.{Type, TypeTag, typeOf}

/**
 * Interface to define a custom Spark-Dataset transformation (2:1)
 * When you implement this interface, you need to provide 3 case classes: 2 for your input Datasets
 * and 1 for your output Dataset.
 */
trait CustomDs2to1Transformer[In1 <: Product, In2 <: Product, Out <: Product] extends Serializable {

  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   *
   * @param session      Spark Session
   * @param options      Options specified in the configuration for this transformation
   * @param inputDS      Input Dataset
   * @param dataObjectId Id of DataObject of SubFeed
   * @return Transformed DataFrame
   */

  def transform(session: SparkSession, options: Map[String, String], d1: Dataset[In1], d2: Dataset[In2]): Dataset[Out]

  def getTypeTag: Type = throw new IllegalArgumentException("When using option \"parameterResolution\" -> \"dataObjectId\"," +
    " you need to define method getTypeTag next to your transform method as follows: override def getTypeTag: universe.Type = typeOf[this.type]")

  private[smartdatalake] def transformBasedOnDataObjectOrder(session: SparkSession, options: Map[String, String], inputDOs: Seq[DataObject], dfs: Map[String, DataFrame])
                                                            (implicit typeTag1: TypeTag[In1], typeTag2: TypeTag[In2]): DataFrame = {
    val inputD1Encoder = org.apache.spark.sql.Encoders.product[In1]
    val inputD2Encoder = org.apache.spark.sql.Encoders.product[In2]

    val inputDO1 = inputDOs.head
    val inputDO2 = inputDOs(1)
    val inputDf1 = dfs(inputDO1.id.id)
    val inputDf2 = dfs(inputDO2.id.id)
    transform(session, options, inputDf1.as(inputD1Encoder), inputDf2.as(inputD2Encoder)).toDF
  }

  private[smartdatalake] def transformBasedOnDataObjectId(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame])
                                                         (implicit typeTag1: TypeTag[In1], typeTag2: TypeTag[In2]): DataFrame = {
    val inputDS1Encoder = org.apache.spark.sql.Encoders.product[In1]
    val inputDS2Encoder = org.apache.spark.sql.Encoders.product[In2]

    val typeTagSubclass = getTypeTag
    val typeTagParentClass = typeOf[this.type]
    val typesOfParamsOfParentTransformMethod = typeTagParentClass.members.filter(_.isMethod).filter(_.fullName.endsWith(".transform")).head.info.paramLists.head.map(_.typeSignature).map(_.typeSymbol)

    //Here we deal with the case when the user has defined 2 functions that are both named transform.
    // He could only do that by defining another method called transform that has a type-signature different from the one that is expected.
    // We only keep the transform method that matches exactly the expected type signature
    val transformMethodsOfSubclass = typeTagSubclass.members.filter(_.isMethod).filter(_.fullName.endsWith(".transform"))
    val transformMethodsToPick = transformMethodsOfSubclass.filter(method => method.info.paramLists.head.map(_.typeSignature).map(_.typeSymbol) == typesOfParamsOfParentTransformMethod)

    assert(transformMethodsToPick.size == 1)

    val transformParameterNames: Seq[String] = transformMethodsToPick.head.info.paramLists.head.map(_.name).map(_.toString)

    //Parameters at index 0 and 1 are sparkSession and options, see method signature of transform
    val firstParameter = transformParameterNames(2)
    val secondParameter = transformParameterNames(3)

    transform(session, options, dfs(firstParameter).as(inputDS1Encoder), dfs(secondParameter).as(inputDS2Encoder)).toDF
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