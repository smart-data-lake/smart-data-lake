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

package io.smartdatalake.workflow.action.snowflake.customlogic

import com.snowflake.snowpark.{DataFrame, Session}
import io.smartdatalake.util.hdfs.PartitionValues

/**
 * Interface to define a custom Snowpark-DataFrame transformation (n:m)
 * Same trait as [[CustomSnowparkDfTransformer]], but multiple input and outputs supported.
 */
trait CustomSnowparkDfsTransformer extends Serializable {

  /**
   * Function to define the transformation between several input and output DataFrames (n:m)
   *
   * @param session the Snowpark Session
   * @param options Options specified in the configuration for this transformation
   * @param dfs DataFrames to be transformed
   * @return Transformed DataFrame
   */
  def transform(session: Session, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame]

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