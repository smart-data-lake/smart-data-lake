/*
 * Smart Data Lake - Build your data lake the smart way.
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
package io.smartdatalake.workflow.dataobject.spark

import io.smartdatalake.config.{ConfigurationException, InstanceRegistry}
import io.smartdatalake.config.SdlConfigObject.ConnectionId
import io.smartdatalake.definitions.Environment
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.workflow.connection.{Connection, SparkClassicConnection}
import io.smartdatalake.workflow.dataframe.GenericDataFrame
import io.smartdatalake.workflow.dataframe.spark.{SparkDataFrame, SparkSubFeed}
import io.smartdatalake.workflow.dataobject.DataObject
import io.smartdatalake.workflow.dataobject.generic.CanCreateDataFrame
import io.smartdatalake.workflow.{ActionPipelineContext, DataFrameSubFeed}
import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.{Type, typeOf}

trait CanCreateSparkDataFrame extends CanCreateDataFrame { this: DataObject =>

  /**
   * Configured options for the Spark [[DataFrameReader]]/[[DataFrameWriter]].
   *
   * @see [[DataFrameReader]]
   * @see [[DataFrameWriter]]
   */
  def options: Map[String, String] = Map()

  def getSparkDataFrame(partitionValues: Seq[PartitionValues] = Seq())(implicit context: ActionPipelineContext) : DataFrame

  override def getDataFrame(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type = SparkSubFeed.subFeedType)(implicit context: ActionPipelineContext) : GenericDataFrame = {
    if (subFeedType =:= typeOf[SparkSubFeed]) SparkDataFrame(getSparkDataFrame(partitionValues))
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeed(partitionValues: Seq[PartitionValues] = Seq(), subFeedType: Type)(implicit context: ActionPipelineContext): DataFrameSubFeed = {
    if (subFeedType =:= typeOf[SparkSubFeed]) SparkSubFeed(Some(SparkDataFrame(getSparkDataFrame(partitionValues))), id, partitionValues)
    else throw new IllegalStateException(s"($id) Unknown subFeedType ${subFeedType.typeSymbol.name}")
  }

  override private[smartdatalake] def getSubFeedSupportedTypes: Seq[Type] = Seq(typeOf[SparkSubFeed])

  def sparkConnectionId: Option[ConnectionId]

  implicit def instanceRegistry: InstanceRegistry

  def sparkConnection(implicit context: ActionPipelineContext): SparkClassicConnection = {
    val connectionId = sparkConnectionId.orElse(context.globalConfig.defaultSparkConnectionId)
      .getOrElse(throw ConfigurationException(s"($id) Missing Spark connection: sparkConnectionId of DataObject and globalConfig.defaultSparkConnectionId are not defined"))
    getConnection[SparkClassicConnection](connectionId)
  }

  override def getEngineConnection(subFeedType: universe.Type)(implicit context: ActionPipelineContext): Option[Connection] = {
    if (subFeedType =:= typeOf[SparkSubFeed]) Some(sparkConnection)
    else throw new IllegalStateException(s"($id) Unsupported subFeedType ${subFeedType.typeSymbol.name} for getting engine connection")
  }
}

