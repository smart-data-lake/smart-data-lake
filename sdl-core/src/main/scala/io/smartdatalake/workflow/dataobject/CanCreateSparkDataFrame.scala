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
package io.smartdatalake.workflow.dataobject

import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

import scala.reflect.runtime.universe.{Type, typeOf}

private[smartdatalake] trait CanCreateSparkDataFrame extends CanCreateDataFrame { this: DataObject =>

  /**
   * Configured options for the Spark [[DataFrameReader]]/[[DataFrameWriter]].
   *
   * @see [[DataFrameReader]]
   * @see [[DataFrameWriter]]
   */
  def options: Map[String, String] = Map()

  def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext) : DataFrame

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext) : GenericDataFrame = {
    if (subFeedType =:= typeOf[SparkSubFeed]) SparkDataFrame(getSparkDataFrame(partitionValues))
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    if (subFeedType =:= typeOf[SparkSubFeed]) SparkSubFeed(Some(SparkDataFrame(getSparkDataFrame(partitionValues))), id, partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkSubFeed])

}

