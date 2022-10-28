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
import io.smartdatalake.util.misc.SmartDataLakeLogger
import io.smartdatalake.workflow.action.spark.transformer.ParameterResolution
import io.smartdatalake.workflow.action.spark.transformer.ParameterResolution.ParameterResolution

/**
 * Interface to define a custom Spark-Dataset transformation with many input Datasets and one output Dataset (n:1)
 * When you implement this interface, you must define one (and only one) function with name "transform" and the following parameters:
 * - 'session: SparkSession'
 * - 'options: Map[String, String]'
 * - as many '<inputDatasetName>: Dataset[<CaseClass>]' as needed
 * The transformer will use reflection to search the "transform" method, and fill in the parameters dynamically.
 * It can match parameters by name or by order, see [[ParameterResolution]].
 */
trait CustomDsNto1Transformer extends Serializable with SmartDataLakeLogger {

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