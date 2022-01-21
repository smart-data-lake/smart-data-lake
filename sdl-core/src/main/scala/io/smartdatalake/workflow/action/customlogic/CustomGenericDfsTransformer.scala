/*
 * Smart Data Lake - Build your data lake the smart way.
 *
 * Copyright © 2019-2020 ELCA Informatique SA (<https://www.elca.ch>)
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
package io.smartdatalake.workflow.action.customlogic

import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.DataFrameSubFeedCompanion
import org.apache.spark.sql.SparkSession

/**
 * Interface to define a custom Spark-DataFrame transformation (n:m)
 * Same trait as [[CustomGenericDfTransformer]], but multiple input and outputs supported.
 */
trait CustomGenericDfsTransformer extends Serializable {

  /**
   * Function to define the transformation between several input and output GenericDataFrames (n:m)
   *
   * @param helper Functions to work with type of the GenericDataFrames given.
   * @param options Options specified in the configuration for this transformation
   * @param dfs GenericDataFrames to be transformed
   * @return Transformed GenericDataFrame
   */
  def transform(helper: DataFrameSubFeedCompanion, options: Map[String,String], dfs: Map[String,GenericDataFrame]) : Map[String,GenericDataFrame]

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

}