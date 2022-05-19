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
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.reflect.runtime.universe.TypeTag

/**
 * Interface to define a custom Spark-Dataset transformation (1:1)
 * When you implement this interface, you need to provide two case classes: One for your input Dataset
 * and one for your output Dataset.
 */
trait CustomDsTransformer[In <: Product, Out <: Product] extends Serializable {

  /**
   * Function to be implemented to define the transformation between an input and output DataFrame (1:1)
   *
   * @param session      Spark Session
   * @param options      Options specified in the configuration for this transformation
   * @param inputDS      Input Dataset
   * @return Transformed DataFrame
   */
  def transform(session: SparkSession, options: Map[String, String], inputDS: Dataset[In]): Dataset[Out]

  private[smartdatalake] def transformWithTypeConversion(session: SparkSession, options: Map[String, String], inputDf: DataFrame, dataObjectId: String)(implicit typeTag: TypeTag[In]): DataFrame = {
    val inputDSEncoder = org.apache.spark.sql.Encoders.product[In]
    transform(session, options, inputDf.as(inputDSEncoder)).toDF
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