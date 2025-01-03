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
package io.smartdatalake.workflow

import io.smartdatalake.config.SdlConfigObject.DataObjectId
import io.smartdatalake.util.dag.DAGResult
import io.smartdatalake.util.hdfs.PartitionValues
import io.smartdatalake.util.misc.{ProductUtil, SmartDataLakeLogger}
import io.smartdatalake.workflow.action.ActionSubFeedsImpl.MetricsMap
import io.smartdatalake.workflow.action.executionMode.ExecutionModeResult

/**
 * A SubFeed transports references to data between Actions.
 * Data can be represented by different technologies like Files or DataFrame.
 */
trait SubFeed extends DAGResult with SmartDataLakeLogger {
  def dataObjectId: DataObjectId
  def partitionValues: Seq[PartitionValues]
  def isDAGStart: Boolean
  def isSkipped: Boolean
  def metrics: Option[MetricsMap]

  /**
   * Break lineage.
   * This means to discard an existing DataFrame or List of FileRefs, so that it is requested again from the DataObject.
   * On one side this is usable to break long DataFrame Lineages over multiple Actions and instead reread the data from
   * an intermediate table. On the other side it is needed if partition values or filter condition are changed.
   */
  def breakLineage(implicit context: ActionPipelineContext): SubFeed

  def clearPartitionValues(breakLineageOnChange: Boolean = true)(implicit context: ActionPipelineContext): SubFeed

  def updatePartitionValues(partitions: Seq[String], breakLineageOnChange: Boolean = true, newPartitionValues: Option[Seq[PartitionValues]] = None)(implicit context: ActionPipelineContext): SubFeed

  def clearDAGStart(): SubFeed = ProductUtil.dynamicCopy(this, "isDAGStart", false)

  def clearSkipped(): SubFeed = ProductUtil.dynamicCopy(this, "isSkipped", false)

  def setSkipped(): SubFeed = ProductUtil.dynamicCopy(this, "isSkipped", true)

  def toOutput(dataObjectId: DataObjectId): SubFeed

  def union(other: SubFeed)(implicit context: ActionPipelineContext): SubFeed

  override def resultId: String = dataObjectId.id

  def unionPartitionValues(otherPartitionValues: Seq[PartitionValues]): Seq[PartitionValues] = {
    // union is only possible if both inputs have partition values defined. Otherwise default is no partition values which means to read all data.
    if (this.partitionValues.nonEmpty && otherPartitionValues.nonEmpty) (this.partitionValues ++ otherPartitionValues).distinct
    else Seq()
  }

  def applyExecutionModeResultForInput(result: ExecutionModeResult, mainInputId: DataObjectId)(implicit context: ActionPipelineContext): SubFeed
  def applyExecutionModeResultForOutput(result: ExecutionModeResult)(implicit context: ActionPipelineContext): SubFeed

  def withMetrics(metrics: MetricsMap): SubFeed = ProductUtil.dynamicCopy(this, "metrics", Some(metrics))

  def appendMetrics(metrics: MetricsMap): SubFeed = withMetrics(this.metrics.getOrElse(Map()) ++ metrics)

}
object SubFeed {
  def filterPartitionValues(partitionValues: Seq[PartitionValues], partitions: Seq[String]): Seq[PartitionValues] = {
    partitionValues.map( pvs => PartitionValues(pvs.elements.filterKeys(partitions.contains).toMap)).filter(_.nonEmpty)
  }
}

/**
 * An interface to be implemented by SubFeed companion objects for subfeed conversion
 */
trait SubFeedConverter[S <: SubFeed] {
  def fromSubFeed(subFeed: SubFeed)(implicit context: ActionPipelineContext): S
  def get(subFeed: SubFeed): S = subFeed match {
    case specificSubFeed: S @unchecked => specificSubFeed
  }
}
